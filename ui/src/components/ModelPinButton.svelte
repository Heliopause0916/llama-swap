<script lang="ts">
  import type { Model } from "../lib/types";
  import { pinModel, unpinModel } from "../stores/api";
  import { Pin, PinOff } from "@lucide/svelte";

  interface Props {
    model: Model;
  }

  let { model }: Props = $props();

  let busy = $state(false);

  async function handleTogglePin(): Promise<void> {
    busy = true;
    try {
      if (model.pinned) {
        await unpinModel(model.id);
      } else {
        await pinModel(model.id);
      }
    } catch (error) {
      console.error("Failed to toggle pin", model.id, error);
    } finally {
      busy = false;
    }
  }
</script>

<button
  type="button"
  class="text-muted-foreground hover:bg-accent hover:text-foreground flex size-7 shrink-0 items-center justify-center rounded-md disabled:opacity-50"
  title={model.pinned ? "Unpin model" : "Pin model"}
  aria-label={model.pinned ? "Unpin model" : "Pin model"}
  disabled={model.state !== "ready" || busy}
  onclick={handleTogglePin}
>
  {#if model.pinned}
    <PinOff class="size-4" />
  {:else}
    <Pin class="size-4" />
  {/if}
</button>
