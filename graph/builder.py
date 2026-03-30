
# ── graph/builder.py ──────────────────────────────────────────────────────────

from langgraph.graph import StateGraph, END
from state.graph_state import GraphState
from agents.scout import scout_node
from agents.analyzer import analyzer_node
from agents.resolver import resolver_node
from agents.architect import architect_plan_node
from agents.generator import generator_node
from agents.reviewer import reviewer_node
from agents.fixer import fixer_node
from agents.documenter import documenter_node
from agents.sttm import sttm_node
from graph.conditions import (
    after_scout,
    after_analyzer,
    after_resolver,
    after_reviewer_fixer,
)
from utils.dbt_writer import write_dbt_project
from utils.logger import log_step
from config.settings import OUTPUTS_DIR


def write_output_node(state: GraphState) -> dict:
    print("\n[WRITE] Writing project to disk...")
    project = state["dbt_project"]
    written = write_dbt_project(project, OUTPUTS_DIR)
    for f in written:
        print(f"  ✓ {f}")
    return {"status": "done"}


def halt_node(state: GraphState) -> dict:
    print("\n[HALT] Pipeline stopped.")
    print(f"  Status : {state.get('status')}")
    print(f"  Error  : {state.get('error', 'See logs for details')}")
    if state.get("resolved_mappings"):
        rm = state["resolved_mappings"]
        if rm.unresolved_tables:
            print(f"  Unresolved tables: {rm.unresolved_tables}")
    return {"status": "halted"}


def build_graph() -> StateGraph:
    """
    Pipeline flow:

        Scout → Analyzer → Resolver → [Architect if dbt] → Generator
              → Reviewer → Fixer (loop) → Write Output
              → Documenter → STTM → END

    Architect is bypassed for non-dbt target platforms.
    """
    graph = StateGraph(GraphState)

    graph.add_node("scout",        scout_node)
    graph.add_node("analyzer",     analyzer_node)
    graph.add_node("resolver",     resolver_node)
    graph.add_node("architect",    architect_plan_node)
    graph.add_node("generator",    generator_node)
    graph.add_node("reviewer",     reviewer_node)
    graph.add_node("fixer",        fixer_node)
    graph.add_node("write_output", write_output_node)
    graph.add_node("documenter",   documenter_node)
    graph.add_node("sttm",         sttm_node)
    graph.add_node("halt",         halt_node)

    graph.set_entry_point("scout")

    # Scout → Analyzer (always) or halt on detection failure
    graph.add_conditional_edges("scout", after_scout, {
        "analyzer": "analyzer",
        "halt":     "halt",
    })

    # Analyzer → Resolver or halt
    graph.add_conditional_edges("analyzer", after_analyzer, {
        "resolver": "resolver",
        "halt":     "halt",
    })

    # Resolver → Architect (dbt) or Generator (all others) or halt
    graph.add_conditional_edges("resolver", after_resolver, {
        "architect": "architect",
        "generator": "generator",
        "halt":      "halt",
    })

    # dbt path: architect always goes to generator
    graph.add_edge("architect", "generator")

    # Generator → Reviewer → Fixer loop → Write Output
    graph.add_edge("generator", "reviewer")
    graph.add_conditional_edges("reviewer", after_reviewer_fixer, {
        "fixer":        "fixer",
        "write_output": "write_output",
    })
    graph.add_edge("fixer", "reviewer")

    # Post-write documentation
    graph.add_edge("write_output", "documenter")
    graph.add_edge("documenter",   "sttm")
    graph.add_edge("sttm",         END)

    graph.add_edge("halt", END)

    return graph.compile()