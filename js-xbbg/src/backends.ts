import { FORMAT_BY_NAME } from './_defs_gen';
import type { BackendKind, FormatKind } from './types';

export const Backend = Object.freeze({
  ARROW: 'arrow',
  JSON: 'json',
  POLARS: 'polars',
}) satisfies Readonly<Record<string, BackendKind>>;

/**
 * Canonical output formats. Names and wire values both come from
 * `defs/bloomberg.toml` via `_defs_gen.ts`.
 *
 * Retyping them here is what shipped `LONG_WITH_METADATA` as
 * `'long_with_metadata'`, a value the Rust engine rejects; its real wire value
 * is `'long_metadata'`.
 */
export const Format = Object.freeze(FORMAT_BY_NAME) satisfies Readonly<Record<string, FormatKind>>;
