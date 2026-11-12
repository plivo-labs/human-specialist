export type Held = { admin: boolean; session: boolean; boss: boolean };
export type Need = keyof Held;

export const holds = (key: boolean, role?: string | null): Held => ({ admin: key || role === "admin", session: role !== undefined, boss: role === "admin" || role === "supervisor" });
export const visible = <T extends { need?: Need }>(items: T[], h: Held) => items.filter((i) => h[i.need ?? "admin"]);
export const landing = (h: Held) => (h.session ? "/desk" : "/agents");
export const renewDue = (ends: number, now: number, tried: number) => now >= ends - 6 * 60e3 && now - tried >= 60e3;

export const NO_PASSWORD = "If you have no password yet, ask an administrator to set your email and password under Settings, Users.";

export function signInTrouble(code: string, retryAfter: unknown): string | null {
  if (code === "invalid_credentials") return `The email or the password is wrong. ${NO_PASSWORD}`;
  if (code === "tenant_required") return "This server has more than one organization, so sign in from your organization's sign-in link. Your admin has it.";
  if (code !== "too_many_attempts") return null;
  const min = Math.ceil(Number(retryAfter) / 60);
  return `Too many tries. Try again in ${min > 1 ? `${min} minutes` : "a minute"}.`;
}
