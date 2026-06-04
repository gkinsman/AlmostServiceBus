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
  state: 'Active' | 'Consumed' | 'DeadLettered'
}

export interface MessageEvent {
  type: 'Enqueued' | 'Completed' | 'DeadLettered' | 'Abandoned' | 'NamespaceCreated'
  namespace: string
  entity: string
  messageId: string
  sequenceNumber: number
  contentType: string | null
  bodyPreview: string | null
  scalarProperties: Record<string, unknown> | null
  timestamp: string
}

export interface EntityGroup {
  prefix: string
  topics: TopicInfo[]
  collapsed: boolean
}
