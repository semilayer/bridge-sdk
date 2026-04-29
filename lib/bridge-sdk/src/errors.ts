import type { WhereLogicalOp, WhereStringOp } from '@semilayer/core'

/**
 * The bridge was asked to evaluate a `where` operator it doesn't support.
 *
 * Bridges declare which logical combinators (`$or`, `$and`, `$not`) and
 * string operators (`$ilike`, `$contains`, `$startsWith`, `$endsWith`) they
 * can push down via `BridgeCapabilities.whereLogicalOps` /
 * `whereStringOps`. When a caller hands a bridge an operator outside the
 * declared set, the bridge throws this error rather than silently ignoring
 * the predicate (which would return wrong results) or pretending to push
 * down in JS (which would burn the entire table).
 *
 * Callers (typically the join planner or a service-level query router) can
 * `instanceof`-check the error and either fall back to a JS-side filter or
 * surface a 400 to the API consumer. The error carries the offending
 * operator and target so the message can be precise without the caller
 * having to reconstruct context.
 */
export class UnsupportedOperatorError extends Error {
  /** The operator that was rejected. Includes the leading `$` so error text matches the spec. */
  readonly op: string
  /** The target (table / collection) the query was against, when known. */
  readonly target?: string
  /** The bridge that rejected the operator, when known. */
  readonly bridge?: string

  constructor(args: {
    op: WhereLogicalOp | WhereStringOp | string
    target?: string
    bridge?: string
    /** Override the default message — useful when the cause is more specific than "operator unsupported". */
    message?: string
  }) {
    const op = args.op.startsWith('$') ? args.op : `$${args.op}`
    const target = args.target ? ` on target "${args.target}"` : ''
    const bridge = args.bridge ? ` (${args.bridge})` : ''
    super(args.message ?? `Bridge does not support where operator "${op}"${target}${bridge}`)
    this.name = 'UnsupportedOperatorError'
    this.op = op
    this.target = args.target
    this.bridge = args.bridge
    // Preserve the prototype chain across transpilation (TS down-emit
    // and Node ESM both lose `instanceof` without this).
    Object.setPrototypeOf(this, UnsupportedOperatorError.prototype)
  }
}
