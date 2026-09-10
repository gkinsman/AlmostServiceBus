<script setup lang="ts">
import { ref } from 'vue'
import type { MessageInfo } from '../types'
import JsonViewer from './JsonViewer.vue'

const props = defineProps<{ message: MessageInfo }>()
const activeTab = ref<'body' | 'appProps' | 'sysProps'>('body')
</script>

<template>
  <div class="detail">
    <div class="detail-header">
      <span class="msg-id">{{ message.messageId }}</span>
    </div>

    <div v-if="message.deadLetterReason || message.deadLetterErrorDescription" class="dead-letter">
      <div class="dl-title">Dead-lettered<span v-if="message.deadLetterSource"> from {{ message.deadLetterSource }}</span></div>
      <div class="dl-reason">{{ message.deadLetterReason ?? 'No reason given' }}</div>
      <div v-if="message.deadLetterErrorDescription" class="dl-desc">{{ message.deadLetterErrorDescription }}</div>
    </div>

    <div class="metadata">
      <div class="meta-row">
        <span class="label">Message ID</span>
        <span class="value mono">{{ message.messageId }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Enqueued</span>
        <span class="value">{{ new Date(message.enqueuedTimeUtc).toLocaleString() }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Content Type</span>
        <span class="value">{{ message.contentType ?? '\u2014' }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Correlation ID</span>
        <span class="value mono">{{ message.correlationId ?? '\u2014' }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Delivery Count</span>
        <span class="value">{{ message.deliveryCount }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Sequence No.</span>
        <span class="value">{{ message.sequenceNumber }}</span>
      </div>
    </div>

    <div class="tabs">
      <div class="tab" :class="{ active: activeTab === 'body' }" @click="activeTab = 'body'">Body</div>
      <div class="tab" :class="{ active: activeTab === 'appProps' }" @click="activeTab = 'appProps'">
        App Properties
        <span v-if="message.applicationProperties" class="count">
          {{ Object.keys(message.applicationProperties).length }}
        </span>
      </div>
      <div class="tab" :class="{ active: activeTab === 'sysProps' }" @click="activeTab = 'sysProps'">System</div>
    </div>

    <div class="tab-content">
      <div v-if="activeTab === 'body'" class="body-viewer">
        <JsonViewer v-if="message.bodyText" :json="message.bodyText" />
        <div v-else class="empty">No body</div>
      </div>

      <div v-if="activeTab === 'appProps'" class="props-viewer">
        <template v-if="message.applicationProperties">
          <div v-for="(val, key) in message.applicationProperties" :key="key" class="prop-row">
            <span class="prop-key">{{ key }}</span>
            <span class="prop-val">{{ JSON.stringify(val) }}</span>
          </div>
        </template>
        <div v-else class="empty">No application properties</div>
      </div>

      <div v-if="activeTab === 'sysProps'" class="props-viewer">
        <div class="prop-row">
          <span class="prop-key">SequenceNumber</span>
          <span class="prop-val">{{ message.sequenceNumber }}</span>
        </div>
        <div class="prop-row">
          <span class="prop-key">DeliveryCount</span>
          <span class="prop-val">{{ message.deliveryCount }}</span>
        </div>
        <div class="prop-row">
          <span class="prop-key">EnqueuedTimeUtc</span>
          <span class="prop-val">{{ message.enqueuedTimeUtc }}</span>
        </div>
        <div v-if="message.subject" class="prop-row">
          <span class="prop-key">Subject</span>
          <span class="prop-val">{{ message.subject }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.detail { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
.detail-header { padding: 10px 16px; border-bottom: 1px solid var(--border); background: var(--bg-crust); }
.msg-id { color: var(--blue); font-weight: 600; font-size: 12px; font-family: 'Cascadia Code', 'Fira Code', monospace; }
.dead-letter { padding: 10px 16px; border-bottom: 1px solid var(--border); background: var(--bg-mantle); border-left: 3px solid var(--red); font-size: 11px; }
.dl-title { color: var(--red); font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; font-size: 10px; }
.dl-reason { color: var(--text); font-weight: 600; margin-top: 3px; font-family: 'Cascadia Code', 'Fira Code', monospace; }
.dl-desc { color: var(--text-muted); margin-top: 2px; }
.metadata { padding: 12px 16px; border-bottom: 1px solid var(--border); font-size: 11px; background: var(--bg-mantle); }
.meta-row { display: grid; grid-template-columns: 120px 1fr; gap: 4px 12px; padding: 3px 0; }
.label { color: var(--text-muted); font-weight: 500; }
.value { color: var(--text); }
.value.mono { font-family: 'Cascadia Code', 'Fira Code', monospace; font-size: 10px; }
.tabs { display: flex; gap: 0; border-bottom: 1px solid var(--border); background: var(--bg-crust); }
.tab { padding: 7px 12px; color: var(--text-muted); font-size: 10px; cursor: pointer; transition: color 0.1s; border-bottom: 2px solid transparent; text-transform: uppercase; letter-spacing: 0.03em; }
.tab:hover { color: var(--text); }
.tab.active { border-bottom-color: var(--blue); color: var(--blue); font-weight: 600; }
.count { background: var(--bg-surface); color: var(--text-muted); padding: 0 5px; border-radius: 3px; font-size: 9px; margin-left: 4px; }
.tab-content { flex: 1; overflow-y: auto; background: var(--bg-mantle); }
.tab-content::-webkit-scrollbar { width: 6px; }
.tab-content::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.tab-content::-webkit-scrollbar-track { background: transparent; }
.body-viewer { padding: 14px 16px; min-height: 100%; }
.props-viewer { padding: 12px 16px; }
.prop-row { display: grid; grid-template-columns: 200px 1fr; gap: 4px 12px; padding: 5px 0; border-bottom: 1px solid var(--border-subtle); font-size: 11px; }
.prop-key { color: var(--blue); font-family: 'Cascadia Code', 'Fira Code', monospace; font-weight: 500; }
.prop-val { color: var(--text); font-family: 'Cascadia Code', 'Fira Code', monospace; word-break: break-all; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
