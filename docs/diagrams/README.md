# Editable architecture diagrams

- messaging-overview: product-level topic/consumer model.
- components: UML-style components of application:from_env, with optional crypto.
- protected-sequence: numbered sequence with the optional encryption region.

The first two SVGs use editable Graphviz .dot sources. The UML sequence has
a Mermaid .mmd source and a deterministic editable render_sequence.py SVG renderer. It is a
static architecture description, not a captured deployment or new test result.

Regenerate with Graphviz:

```bash
for name in messaging-overview components; do
  dot -Tsvg docs/diagrams/$name.dot -o docs/diagrams/$name.svg
done
python docs/diagrams/render_sequence.py
```

See ../PAYLOAD_PROTECTION.md for exact activation and trust boundaries. The
sequence is a lifeline-based UML interaction view, not a promise of
business completion at broker ACK. Sources contain no credentials or deployment data.
