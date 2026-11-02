<script lang="ts">
  import type { Model } from "../lib/types";
  import { enterMaintenance, exitMaintenance } from "../stores/api";
  import { Wrench, WrenchOff } from "@lucide/svelte";

  interface Props {
    model: Model;
  }

  let { model }: Props = $props();

  let busy = $state(false);

  async function handleToggleMaintenance(): Promise<void> {
    busy = true;
    try {
      if (model.maintenance) {
        await exitMaintenance(model.id);
      } else {
        await enterMaintenance(model.id);
      }
    } catch (error) {
      console.error("Failed to toggle maintenance", model.id, error);
    } finally {
      busy = false;
    }
  }
</script>

<button
  type="button"
  class="text-muted-foreground hover:bg-accent hover:text-foreground flex size-7 shrink-0 items-center justify-center rounded-md disabled:opacity-50"
  title={model.maintenance ? "Resume model" : "Enter maintenance"}
  aria-label={model.maintenance ? "Resume model" : "Enter maintenance"}
  disabled={model.state !== "stopped" || busy}
  onclick={handleToggleMaintenance}
>
  {#if model.maintenance}
    <WrenchOff class="size-4" />
  {:else}
    <Wrench class="size-4" />
  {/if}
</button>
