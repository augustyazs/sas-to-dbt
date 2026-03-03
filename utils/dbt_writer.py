from pathlib import Path
from models.schemas import DbtProject


def write_dbt_project(project: DbtProject, output_dir: Path) -> list[str]:
    """Write all dbt project files to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []

    file_map = {
        "dbt_project.yml": project.dbt_project_yml,
        "models/sources.yml": project.sources_yml,
        "models/schema.yml": project.schema_yml,
    }

    for rel_path, content in file_map.items():
        if content and content.strip():
            fp = output_dir / rel_path
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(content, encoding="utf-8")
            written.append(str(fp))

    for model in project.models:
        fp = output_dir / model.path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(model.content, encoding="utf-8")
        written.append(str(fp))

    for macro in project.macros:
        fp = output_dir / macro.path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(macro.content, encoding="utf-8")
        written.append(str(fp))

    if project.not_converted:
        fp = output_dir / "NOT_CONVERTED.md"
        lines = ["# Blocks Not Converted to dbt\n"]
        for item in project.not_converted:
            lines.append(f"- {item}\n")
        fp.write_text("\n".join(lines), encoding="utf-8")
        written.append(str(fp))

    return written