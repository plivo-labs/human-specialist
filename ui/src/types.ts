export type Named = { id: string; name: string | null };
export type Status = { id: string; name: string };

export type Agent = {
  id: string;
  name: string;
  email?: string | null;
  role: "admin" | "supervisor" | "agent" | null;
  teams: string[];
  status: Status | null;
  caller_id: string | null;
  endpoint_username: string | null;
};

export type Me = Agent & {
  grants: { monitor: boolean; barge: boolean };
  settings: { agent_direct_dial: boolean; wrap_up: { mode: "manual" | "auto"; seconds: number }; recording?: { inbound: boolean; outbound: boolean; allow_pause: boolean } };
  pending_offers: { offer_id: string; conversation_id: string; channel: string; from: string | null; via: string; offered_at: string; expires_at: string }[];
};

export type PresenceStatus = { id: string; name: string; availability: boolean; color?: string; sequence: number; default?: boolean };

export type Disposition = {
  id: string;
  name: string;
  note_required: boolean;
  parent_disposition_id: string | null;
  system: boolean;
  disabled?: boolean;
  sequence: number;
};

export type Queue = {
  id: string;
  name: string;
  agents?: string[];
  strategy?: "availability" | "simultaneous" | "fewest_handled" | "least_handle_time";
  wait_timeout_sec?: number;
  enqueue_url: string;
  priority: number;
  capacity: number;
  enabled: boolean;
};

export type Action = { type: string; queue_id?: string; [more: string]: unknown };
export type EntryPoint = { id: string; number: string; open_action: Action; business_hours_id: string | null; closed_action: Action | null };

export type Assignment = {
  agent_id: string | null;
  role: string;
  via: string;
  mode: string | null;
  state: "active" | "wrap_up";
  started_at: string;
  ended_at: string | null;
  wrap_up_until: string | null;
};

export type ConversationSummary = {
  id: string;
  channel: string;
  direction: "inbound" | "outbound";
  state: "active" | "transferring" | "in_wrap_up" | "closed";
  number: string | null;
  our_address?: string | null;
  queue_id?: string | null;
  queue_name?: string | null;
  agent_ids: string[];
  conversation_number: string;
  created_at: string;
  ended_at: string | null;
  last_activity_at?: string | null;
  last_message?: { text: string | null } | null;
  unread_count?: number;
};

export type Conversation = ConversationSummary & {
  context: Record<string, unknown> | null;
  assignments: Assignment[];
  dispositions: { agent_id: string | null; disposition?: string | null; sub_disposition?: string | null; skipped: boolean }[];
};

export type Message = {
  id: string;
  direction: "inbound" | "outbound";
  text: string | null;
  attachments: { name?: string | null }[];
  email: { subject?: string | null } | null;
  status: string;
  created_at: string;
};

export type Recording = { id: string; audio_url: string; duration_sec: number | null; created_at: string };

export type Envelope = {
  id: string;
  type: string;
  created_at: string;
  conversation_id: string | null;
  agent_id: string | null;
  direction?: "inbound" | "outbound" | null;
  data: Record<string, any>;
};

export type Session = {
  agent_token: string;
  stream_token: string;
  expires_at: string;
  renewable_until?: string;
  plivo: { access_token: string };
};

export type TransferTargets = {
  agents: { id: string; name: string | null; availability: "available" | "busy" | "unavailable"; status: Status | null }[];
  teams: { id: string; name: string | null; available_agents: number }[];
  destinations: Named[];
};

export type Floor = {
  agents: { agent_id: string; activity: "idle" | "ringing" | "on_call" | "wrap_up"; status: Status | null; conversation_id: string | null }[];
  queues: { queue_id: string; name: string; waiting: number; oldest_wait_sec: number }[];
};

export type Skill = Named & { min_level: number; max_level: number };
export type Destination = Named & { number: string | null; url: string | null };
export type Slot = { day: string; start: string; end: string };
export type Holiday = { date: string; name: string; slots?: unknown[] };
export type HolidayGroup = Named & { holidays: Holiday[] };
export type BusinessHours = Named & { time_zone: string; slots: Slot[]; holiday_group_id: string | null; selected_holidays: string[] };
export type Task = {
  id: string;
  conversation_id: string;
  title: string;
  description: string | null;
  customer: string | null;
  type: string;
  channel: string | null;
  priority: "low" | "medium" | "high" | "critical";
  status: string;
  created_at: string;
  due_at: string | null;
};
