"""Validate release identity and emit provenance without accessing credentials."""
import json
import pathlib
import subprocess
import tomllib

root = pathlib.Path(__file__).resolve().parents[1]
upstream = json.loads((root / 'ogn/upstream.json').read_text())
manifest = tomllib.loads((root / 'application/Cargo.toml').read_text())
version = upstream['version'] + '+ogn.' + str(upstream['ogn_revision'])
assert manifest['package']['version'] == version, 'Release version and provenance disagree'
commit = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
subprocess.run(['git', '-C', str(root), 'merge-base', '--is-ancestor', upstream['commit'], 'HEAD'], check=True)
print(json.dumps({'version': version, 'fork_repository': 'mike9918/ogn-wings', 'fork_commit': commit, 'upstream': upstream}, indent=2))
