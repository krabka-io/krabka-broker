# Imported Rust paths

Rust code should import an item where it is used instead of referring to it
through `crate::...`. In code, use the imported item name or an imported
`module::item` path. Use declarations, documentation, attributes, and macro
bodies are outside this rule because their path resolution has different
semantics.

Run the check locally with:

```sh
python3 -m venv .venv-imports
.venv-imports/bin/pip install -r tools/imports/requirements.txt
.venv-imports/bin/python tools/imports/check_imported_paths.py
```

CI runs the same check in the `jscpd` job.
