export const operatorKey = (v: FormDataEntryValue | null) => String(v).trim().replace(/^HS_OPERATOR_KEY=/, "");
export const phone = (n?: string | null) =>
  /^\+1\d{10}$/.test(n ?? "") ? `+1 ${n!.slice(2, 5)} ${n!.slice(5, 8)} ${n!.slice(8)}` : n || "Unknown number";

export function clock(seconds: number) {
  const s = Math.max(0, Math.floor(seconds));
  const h = Math.floor(s / 3600);
  const m = Math.floor(s / 60) % 60;
  const ss = String(s % 60).padStart(2, "0");
  return h ? `${h}:${String(m).padStart(2, "0")}:${ss}` : `${m}:${ss}`;
}

export const time = (iso: string) => {
  const d = new Date(iso);
  const day = d.toDateString() === new Date().toDateString() ? {} : ({ month: "short", day: "numeric" } as const);
  return d.toLocaleString("en-US", { ...day, hour: "numeric", minute: "2-digit" });
};

export const when = (iso: string) => new Date(iso).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });

export const dmy = (iso: string) => {
  const d = new Date(iso);
  return `${String(d.getDate()).padStart(2, "0")}-${d.toLocaleString("en-US", { month: "short" })}-${d.getFullYear()}`;
};

export const zone = (z: string) => [z.split("/").pop()!.replaceAll("_", " "), `(${new Intl.DateTimeFormat("en-US", { timeZone: z, timeZoneName: "longOffset" }).formatToParts().find((p) => p.type === "timeZoneName")!.value.replace("GMT", "UTC").replace(/([+-])0/, "$1")})`];

export const day = (iso?: string | null) => (iso ? new Date(iso).toLocaleDateString([], { month: "short", day: "2-digit", year: "numeric" }) : "--");

const UNITS: [Intl.RelativeTimeFormatUnit, number][] = [["day", 86400], ["hour", 3600], ["minute", 60]];
export function ago(iso: string) {
  const sec = (Date.parse(iso) - Date.now()) / 1000;
  const [unit, size] = UNITS.find(([, n]) => Math.abs(sec) >= n) ?? ["second", 1];
  return new Intl.RelativeTimeFormat([], { numeric: "auto" }).format(Math.round(sec / size), unit);
}

export const plain = (code: unknown) => String(code ?? "").replaceAll("_", " ");

export const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

type Fields = { fields?: { loc?: (string | number)[]; msg?: string }[]; used_by?: { kind: string; number?: string }[]; waiting?: number; scheduled?: number; status?: number; errors?: Record<string, string | string[]> } | null | undefined;
export const explain = (message: string, details: Fields) =>
  [
    message,
    ...(details?.fields ?? []).map(({ loc = [], msg = "" }) => {
      const at = loc.filter((l) => typeof l === "string" && l !== "body").at(-1);
      const why = plain(msg.replace(/^(Value error|Assertion failed), /, ""));
      return at ? `${cap(plain(at))}: ${why}` : cap(why);
    }),
    ...(details?.used_by?.length ? [`Used by ${details.used_by.map((u) => (u.number ? phone(u.number) : `a ${plain(u.kind)}`)).join(", ")}`] : []),
    ...(details?.waiting ? [`${details.waiting} waiting. Disable the queue, and delete it once they are taken or closed`] : []),
    ...(details?.scheduled ? [`${details.scheduled} scheduled for it. Give them another target or cancel them`] : []),
    ...Object.values(details?.errors ?? {}).flat().map((e) => `${e}${details?.status ? ` (${details.status})` : ""}`),
  ]
    .map((s) => s.replace(/\.$/, ""))
    .join(". ");
