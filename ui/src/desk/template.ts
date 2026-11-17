import type { Template } from "../types";

export type Hole = { part: "header" | "body" | "button"; label: string; media?: boolean; index?: number };

const HOLES = /{{\s*\w+\s*}}/g;
const MEDIA = new Set(["IMAGE", "VIDEO", "DOCUMENT"]);

export const holes = (t: Template): Hole[] =>
  t.components.flatMap((c): Hole[] => {
    const part = c.type.toLowerCase();
    if (part === "header" && MEDIA.has(c.format ?? "")) return [{ part, label: `Header ${c.format!.toLowerCase()} URL`, media: true }];
    if (part === "header" || part === "body") return (c.text?.match(HOLES) ?? []).map((h) => ({ part, label: `${part === "body" ? "Body" : "Header"} ${h}` }));
    if (part === "buttons") return (c.buttons ?? []).flatMap((b, index) => (b.url?.match(HOLES) ?? []).map((h) => ({ part: "button" as const, index, label: `${b.text} link ${h}` })));
    return [];
  });

export function fill(t: Template, values: string[]) {
  const all = holes(t);
  const params = (of: (h: Hole) => boolean) => all.flatMap((h, i) => (of(h) ? [h.media ? { type: "media", media: values[i] } : { type: "text", text: values[i] }] : []));
  const buttons = [...new Set(all.flatMap((h) => (h.part === "button" ? [h.index!] : [])))];
  const components = [
    { type: "header", parameters: params((h) => h.part === "header") },
    { type: "body", parameters: params((h) => h.part === "body") },
    ...buttons.map((index) => ({ type: "button", sub_type: "url", index: String(index), parameters: params((h) => h.part === "button" && h.index === index) })),
  ];
  return { name: t.name, language: t.language, namespace: t.namespace, components: components.filter((c) => c.parameters.length) };
}
