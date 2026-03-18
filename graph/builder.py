from langgraph.graph import StateGraph, END
from state.graph_state import GraphState
from agents.analyzer import analyzer_node
from agents.resolver import resolver_node
from agents.architect import architect_plan_node, architect_review_node
from agents.generator import generator_node
from agents.reviewer import reviewer_node
from graph.conditions import after_analyzer, after_resolver, after_reviewer
from utils.dbt_writer import write_dbt_project
from utils.logger import log_step
from config.settings import OUTPUTS_DIR


def write_output_node(state: GraphState) -> dict:
    """Write final dbt project to disk."""
    print("\n[WRITE] Writing dbt project to disk...")
    project = state["dbt_project"]
    written = write_dbt_project(project, OUTPUTS_DIR)
    for f in written:
        print(f"  ✓ {f}")
    return {"status": "done"}


def halt_node(state: GraphState) -> dict:
    """Halt pipeline with error details."""
    print("\n[HALT] Pipeline stopped.")
    print(f"  Status: {state.get('status')}")
    print(f"  Error: {state.get('error', 'See resolver/analyzer output for details')}")
    if state.get("resolved_mappings"):
        rm = state["resolved_mappings"]
        if rm.unresolved_tables:
            print(f"  Unresolved tables: {rm.unresolved_tables}")
    return {"status": "halted"}


def build_graph() -> StateGraph:
    """Construct the LangGraph state machine."""
    graph = StateGraph(GraphState)

    graph.add_node("analyzer", analyzer_node)
    graph.add_node("resolver", resolver_node)
    graph.add_node("architect", architect_plan_node)
    graph.add_node("generator", generator_node)
    graph.add_node("architect_review", architect_review_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("write_output", write_output_node)
    graph.add_node("halt", halt_node)

    graph.set_entry_point("analyzer")

    graph.add_conditional_edges("analyzer", after_analyzer, {
        "resolver": "resolver",
        "halt": "halt",
    })

    graph.add_conditional_edges("resolver", after_resolver, {
        "generator": "architect",
        "halt": "halt",
    })

    graph.add_edge("architect", "generator")
    graph.add_edge("generator", "architect_review")
    graph.add_edge("architect_review", "reviewer")

    graph.add_conditional_edges("reviewer", after_reviewer, {
        "reviewer": "reviewer",
        "write_output": "write_output",
    })

    graph.add_edge("write_output", END)
    graph.add_edge("halt", END)

    return graph.compile()
