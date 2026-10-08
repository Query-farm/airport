#include "airport_table_in_out.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb
{
  namespace
  {
    struct AirportInOutGlobalState : GlobalOperatorState
    {
      AirportInOutGlobalState(ClientContext &context, PhysicalOperator &function_p)
          : function(function_p), thread(context)
      {
        function.op_state = function.GetGlobalOperatorState(context);
        ExecutionContext execution(context, thread, nullptr);
        local_state = function.GetOperatorState(execution);
      }

      void Cancel()
      {
        cancelled = true;
        local_state.reset();
        function.op_state.reset();
      }

      PhysicalOperator &function;
      ThreadContext thread;
      unique_ptr<OperatorState> local_state;
      bool cancelled = false;
    };

    struct AirportInOutLocalState : OperatorState
    {
      explicit AirportInOutLocalState(AirportInOutGlobalState &exchange_p) : exchange(exchange_p) {}

      void Finalize(const PhysicalOperator &, ExecutionContext &) override
      {
        // DuckDB skips flushing operators upstream of a satisfied LIMIT (or
        // another downstream operator that finishes early). Cancel the exchange
        // instead of consuming the remaining input or its final output.
        if (!input_exhausted)
        {
          exchange.Cancel();
        }
      }

      AirportInOutGlobalState &exchange;
      bool input_exhausted = false;
    };

    struct AirportInOutSourceState : GlobalSourceState
    {
      bool finished = false;
    };

    // Execute stays in the input pipeline and returns each response immediately.
    // A dependent source pipeline emits final output after all input pipelines
    // finish, like an outer join's final scan. No input or result materialization
    // is needed, and UNION ALL cannot close the writer between its branches.
    class PhysicalAirportTableInOut : public PhysicalOperator
    {
    public:
      PhysicalAirportTableInOut(PhysicalPlan &plan, PhysicalOperator &function_p)
          : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, function_p.types,
                             function_p.estimated_cardinality), function(function_p)
      {
        children.push_back(function.children[0]);
      }

      string GetName() const override { return "AIRPORT_TABLE_IN_OUT"; }
      bool IsSource() const override { return true; }
      bool RequiresFinalExecute() const override { return true; }
      OrderPreservationType OperatorOrder() const override { return OrderPreservationType::FIXED_ORDER; }

      unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override
      {
        return make_uniq<AirportInOutGlobalState>(context, function);
      }

      unique_ptr<OperatorState> GetOperatorState(ExecutionContext &) const override
      {
        return make_uniq<AirportInOutLocalState>(op_state->Cast<AirportInOutGlobalState>());
      }

      OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &output,
                                 GlobalOperatorState &global, OperatorState &) const override
      {
        auto &state = global.Cast<AirportInOutGlobalState>();
        if (state.cancelled)
        {
          return OperatorResultType::FINISHED;
        }
        return function.Execute(context, input, output, *function.op_state, *state.local_state);
      }

      OperatorFinalizeResultType FinalExecute(ExecutionContext &, DataChunk &, GlobalOperatorState &,
                                              OperatorState &local) const override
      {
        // This only marks this input pipeline complete; closing the writer here
        // would still be premature when another UNION branch has more input.
        local.Cast<AirportInOutLocalState>().input_exhausted = true;
        return OperatorFinalizeResultType::FINISHED;
      }

      unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &) const override
      {
        return make_uniq<AirportInOutSourceState>();
      }

      SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &output,
                                       OperatorSourceInput &source) const override
      {
        auto &state = op_state->Cast<AirportInOutGlobalState>();
        auto &final = source.global_state.Cast<AirportInOutSourceState>();
        if (state.cancelled || final.finished)
        {
          return SourceResultType::FINISHED;
        }
        auto result = function.FinalExecute(context, output, *function.op_state, *state.local_state);
        final.finished = result == OperatorFinalizeResultType::FINISHED;
        return final.finished ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
      }

      void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override
      {
        op_state.reset();
        function.op_state.reset();
        auto &state = meta_pipeline.GetState();
        state.AddPipelineOperator(current, *this);

        vector<shared_ptr<Pipeline>> pipelines;
        meta_pipeline.GetPipelines(pipelines, false);
        auto &last_pipeline = *pipelines.back();
        children[0].get().BuildPipelines(current, meta_pipeline);
        meta_pipeline.CreateChildPipeline(current, *this, last_pipeline);
      }

      vector<const_reference<PhysicalOperator>> GetSources() const override
      {
        auto sources = children[0].get().GetSources();
        sources.push_back(*this);
        return sources;
      }

    private:
      PhysicalOperator &function;
    };

    class LogicalAirportTableInOut : public LogicalExtensionOperator
    {
    public:
      explicit LogicalAirportTableInOut(unique_ptr<LogicalOperator> function_p)
          : function(std::move(function_p))
      {
        function->ResolveOperatorTypes();
        auto &get = function->Cast<LogicalGet>();
        if (!get.projected_input.empty())
        {
          throw NotImplementedException("Airport: correlated table-input exchanges are not supported");
        }
        children = std::move(function->children);
        // All input columns are required by the server, even when the query
        // projects only some output columns. Keep those dependencies visible
        // to DuckDB's column pruning and column binding resolver.
        auto bindings = children[0]->GetColumnBindings();
        for (idx_t i = 0; i < bindings.size(); ++i)
        {
          expressions.push_back(make_uniq<BoundColumnRefExpression>(children[0]->types[i], bindings[i]));
        }
      }

      vector<ColumnBinding> GetColumnBindings() override { return function->GetColumnBindings(); }
      vector<idx_t> GetTableIndex() const override { return function->GetTableIndex(); }
      bool SupportSerialization() const override { return false; }
      string GetExtensionName() const override { return "airport"; }
      void Serialize(Serializer &) const override
      {
        // Copying a CTE falls back to materialization for unsupported plans.
        throw NotImplementedException("Airport table-input exchanges cannot be serialized");
      }

      PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override
      {
        // Keep the LogicalGet out of the optimizer tree: DuckDB otherwise
        // removes it for an empty input, losing the exchange's final output.
        function->children = std::move(children);
        function->ResolveOperatorTypes();
        auto &physical_function = planner.CreatePlan(*function);
        return planner.Make<PhysicalAirportTableInOut>(physical_function);
      }

    protected:
      void ResolveTypes() override { types = function->types; }

    private:
      unique_ptr<LogicalOperator> function;
    };

    void PlanTableInOut(unique_ptr<LogicalOperator> &op)
    {
      for (auto &child : op->children)
      {
        PlanTableInOut(child);
      }
      if (op->type == LogicalOperatorType::LOGICAL_GET &&
          IsAirportTableInOutFunction(op->Cast<LogicalGet>().function))
      {
        op = make_uniq<LogicalAirportTableInOut>(std::move(op));
      }
    }
  }

  void AirportPlanTableInOut(PlannerExtensionInput &, BoundStatement &statement)
  {
    if (statement.plan)
    {
      PlanTableInOut(statement.plan);
    }
  }
}
