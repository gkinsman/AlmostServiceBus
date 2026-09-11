<script setup lang="ts">
// One place that decides which icon stands for which kind of thing, so the sidebar, the
// entity header and any future view agree. Icons come from lucide (lucide-vue-next):
// consistent stroke weight, tree-shaken per icon, and they inherit `currentColor`.
import { computed } from 'vue'
import { Inbox, Radio, CornerDownRight, MailWarning, Layers, Globe } from 'lucide-vue-next'
import type { EntityType } from '../types'

export type IconKind = EntityType | 'deadletter' | 'session' | 'namespace'

const props = withDefaults(defineProps<{ type: IconKind | null | undefined; size?: number }>(), { size: 13 })

const icon = computed(() => {
  switch (props.type) {
    case 'queue': return Inbox
    case 'topic': return Radio
    case 'subscription': return CornerDownRight
    case 'deadletter': return MailWarning
    case 'session': return Layers
    case 'namespace': return Globe
    default: return null
  }
})

const label = computed(() => {
  switch (props.type) {
    case 'queue': return 'Queue'
    case 'topic': return 'Topic'
    case 'subscription': return 'Subscription'
    case 'deadletter': return 'Dead-letter queue'
    case 'session': return 'Session-enabled'
    case 'namespace': return 'Namespace'
    default: return ''
  }
})
</script>

<template>
  <component :is="icon" v-if="icon" class="entity-icon" :size="size" :stroke-width="2" :aria-label="label" :title="label" />
</template>

<style scoped>
.entity-icon { flex-shrink: 0; opacity: 0.85; vertical-align: -2px; }
</style>
