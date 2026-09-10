export interface EmulatorInfo {
  connectionString: string
  amqpPort: number
  managementPort: number
  dashboardPort: number
}

export interface NamespaceInfo {
  name: string
  queueCount: number
  topicCount: number
  lastActivityAt: string
}

export interface EntityOverview {
  queues: QueueInfo[]
  topics: TopicInfo[]
}

export interface QueueInfo {
  name: string
  messageCount: number
  deadLetterCount: number
  totalMessageCount: number
  consumedCount: number
  maxDeliveryCount: number
  forwardTo: string | null
}

export interface TopicInfo {
  name: string
  subscriptions: SubscriptionInfo[]
}

export interface SubscriptionInfo {
  name: string
  forwardTo: string | null
  messageCount: number
  ruleCount: number
}

export interface MessageInfo {
  messageId: string
  sequenceNumber: number
  contentType: string | null
  correlationId: string | null
  deliveryCount: number
  enqueuedTimeUtc: string
  subject: string | null
  applicationProperties: Record<string, unknown> | null
  bodyText: string | null
  scalarProperties: Record<string, unknown> | null
  state: 'Active' | 'Consumed' | 'DeadLettered' | 'Deferred'
  deadLetterReason?: string | null
  deadLetterErrorDescription?: string | null
  deadLetterSource?: string | null
}

/** GET /namespaces/{ns}/queues/{name}/properties. Durations are ISO 8601; null means unbounded. */
export interface QueueProperties {
  name: string
  lockDuration: string
  maxDeliveryCount: number
  requiresSession: boolean
  defaultMessageTimeToLive: string | null
  deadLetteringOnMessageExpiration: boolean
  requiresDuplicateDetection: boolean
  duplicateDetectionHistoryTimeWindow: string | null
  enableBatchedOperations: boolean
  enablePartitioning: boolean
  enableExpress: boolean
  maxSizeInMegabytes: number
  autoDeleteOnIdle: string | null
  forwardTo: string | null
  forwardDeadLetteredMessagesTo: string | null
  userMetadata: string | null
  messageCount: number
  deadLetterCount: number
  totalMessageCount: number
  consumedCount: number
  sessionCount: number
}

export interface MessageEvent {
  type: 'Enqueued' | 'Completed' | 'DeadLettered' | 'Abandoned' | 'Deferred' | 'NamespaceCreated'
  namespace: string
  entity: string
  messageId: string
  sequenceNumber: number
  contentType: string | null
  bodyPreview: string | null
  scalarProperties: Record<string, unknown> | null
  timestamp: string
  /** Only on Enqueued events. */
  applicationProperties?: Record<string, unknown> | null
  subject?: string | null
  correlationId?: string | null
}

export interface EntityGroup {
  prefix: string
  topics: TopicInfo[]
  collapsed: boolean
}
