import { tool, type StructuredToolInterface } from "@langchain/core/tools";
import { zodToJsonSchema } from "zod-to-json-schema";
import type * as z from "zod/v3";

import type { BloombergToolName } from "./options";
import { throwWithToolContext, type ToolContentAndArtifact } from "./result-limits";

type ZodOutput<T> = z.ZodType<T, z.ZodTypeDef, unknown>;

/**
 * Subset of the LangChain runnable config forwarded to tool functions.
 * `signal` aborts the call: the LangChain wrapper rejects immediately, and
 * Bloomberg tool functions use it to stop waiting and release subscriptions.
 */
export interface ToolInvocationConfig {
  readonly signal?: AbortSignal;
}

interface BloombergStructuredToolFields<Input> {
  readonly description: string;
  readonly name: BloombergToolName;
  readonly responseFormat: "content_and_artifact";
  readonly schema: ZodOutput<Input>;
}

function inputJsonSchema(schema: ZodOutput<unknown>): Record<string, unknown> {
  const jsonSchema = zodToJsonSchema(schema, {
    $refStrategy: "none",
    effectStrategy: "input",
    pipeStrategy: "input",
  }) as Record<string, unknown>;
  delete jsonSchema.$schema;
  delete jsonSchema.definitions;
  return jsonSchema;
}

/**
 * Provider-ready JSON Schema for a Bloomberg tool's input parameters, using
 * the same conversion settings as the embedded provider tool definition
 * ($ref-free, input-side of transforms). Exposed so consumers do not each
 * reinvent zod -> JSON Schema conversion and sanitization.
 */
export function toolParameterJsonSchema(
  toolInstance: StructuredToolInterface,
): Record<string, unknown> {
  const schema: unknown = toolInstance.schema;
  if (schema !== null && typeof schema === "object" && !("safeParse" in schema)) {
    // Already a JSON Schema object.
    return schema as Record<string, unknown>;
  }
  return inputJsonSchema(schema as ZodOutput<unknown>);
}

export function createBloombergStructuredTool<Input>(
  func: (input: Input, config?: ToolInvocationConfig) => Promise<ToolContentAndArtifact>,
  fields: BloombergStructuredToolFields<Input>,
): StructuredToolInterface {
  const providerToolDefinition = {
    type: "function",
    function: {
      description: fields.description,
      name: fields.name,
      parameters: inputJsonSchema(fields.schema),
    },
  };

  const guarded = async (
    input: Input,
    config?: ToolInvocationConfig,
  ): Promise<ToolContentAndArtifact> => {
    try {
      // Refuse to start Bloomberg work for calls that are already cancelled.
      config?.signal?.throwIfAborted();
    } catch (error) {
      throwWithToolContext(fields.name, error);
    }
    return await func(input, config);
  };

  return tool(
    guarded as never,
    {
      ...fields,
      extras: { providerToolDefinition },
    } as never,
  ) as StructuredToolInterface;
}
