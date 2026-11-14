# Stencil ownership-region policies

## Policy contract

Ownership-region selection is a separate concern from board replay and exact
cover solving. The \`OwnershipRegionPolicy\` protocol receives a validated
\`OwnershipEvidence\` value and returns an \`OwnershipRegions\` value. It owns
only the conversion from ownership evidence to signed regions. The region
solver consumes the result without knowing which policy produced it.

A policy declares its stable identifier, whether it requires KataGo's
ownership-stdev map, validates per-query capability metadata, and classifies
the evidence. New policies belong in sibling modules and implement this
contract; the public transformer and region solver do not contain the policy
algorithm.

The initial implementation is \`threshold\`, selected with
\`KATAPROXY_STENCIL_OWNERSHIP_POLICY=threshold\`. The selection is read through
\`sproxy_config\` at process startup.

## Legacy threshold policy

The former behavior is retained: each empty point is selected independently
when its signed ownership crosses the client-supplied
\`truncation_threshold\`. A zero threshold selects strictly positive or
negative values and leaves exact zero neutral. Dead Black chains are assigned
to White-owned territory; dead White chains are assigned to Black-owned
territory; living stones are excluded.

Example:

\`\`\`json
{
  "capabilities": {
    "stencil_cover": {
      "truncation_threshold": 0.6
    }
  }
}
\`\`\`

The threshold policy does not request or require \`ownershipStdev\`.
