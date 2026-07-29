import type { PiProtocolManifest } from "@kybernetria/pi-protocol";
import { GENERATED_RUN_PROVIDE, generatedNodeId } from "../schemas.ts";
import type { DependencySnapshot, PipelineSpecV1 } from "../types.ts";

export function createGeneratedManifest(spec: PipelineSpecV1, dependencies: readonly DependencySnapshot[] = spec.dependencies ?? []): PiProtocolManifest {
  const effects = [...new Set(["protocol_invoke", ...dependencies.flatMap((dependency) => dependency.effects)])].sort();
  const stepSummary = spec.steps.map((step) => step.target).join(" -> ");
  return {
    protocolVersion: "0.2.0",
    nodeId: generatedNodeId(spec.id),
    packageId: `pi-pe/generated/${spec.id}`,
    version: spec.version,
    purpose: spec.description,
    tags: [...new Set(["pipeline", "generated", ...spec.tags])],
    display: {
      label: spec.name,
      accentToken: "accent",
      outputToken: "toolOutput",
      resultMode: "pipeline",
    },
    provides: [{
      name: GENERATED_RUN_PROVIDE,
      description: `${spec.description} Fixed pipeline: ${stepSummary}.`,
      version: spec.version,
      tags: [...new Set(["pipeline", "generated", ...spec.tags])],
      inputSchema: spec.inputSchema,
      outputSchema: spec.outputSchema,
      execution: { type: "handler", handler: GENERATED_RUN_PROVIDE },
      effects,
      display: {
        label: spec.name,
        accentToken: "accent",
        outputToken: "toolOutput",
        resultMode: "pipeline",
      },
    }],
  };
}
