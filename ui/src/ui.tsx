import * as Dialog from "@radix-ui/react-dialog";
import * as RTabs from "@radix-ui/react-tabs";
import { ChevronDown, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, CircleAlert, CircleCheck, GripVertical, Info, Loader2, Plus, RefreshCcw, Search, X } from "lucide-react";
import {
  type ButtonHTMLAttributes,
  type ComponentProps,
  type FormEvent,
  type InputHTMLAttributes,
  type ReactNode,
  type SelectHTMLAttributes,
  type TextareaHTMLAttributes,
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { type Page as Paged, reason } from "./api";
import { cap, time } from "./format";

export const cls = (...c: (string | false | null | undefined)[]) => c.filter(Boolean).join(" ");

const VARIANT = {
  primary: "bg-primary text-primary-foreground shadow-sm hover:bg-primary/90",
  outline: "border bg-background shadow-sm hover:bg-muted",
  ghost: "hover:bg-muted",
  tertiary: "bg-st2 text-primary shadow-sm hover:bg-st2/80 aria-pressed:bg-primary aria-pressed:text-primary-foreground",
  danger: "border border-destructive text-destructive shadow-sm hover:bg-destructive/10",
  success: "bg-available text-white shadow-sm hover:bg-available/80",
};
const SIZE = { md: "h-9 px-4 pt-0.5 [&_svg]:size-3.5", sm: "h-[30px] px-3 pt-0.5 [&_svg]:size-3.5", xs: "h-6 px-2 pt-0.5 [&_svg]:size-3.5", icon: "size-8 rounded-full [&_svg]:size-4" };

type ButtonProps = Omit<ButtonHTMLAttributes<HTMLButtonElement>, "onClick"> & {
  variant?: keyof typeof VARIANT;
  size?: keyof typeof SIZE;
  busy?: boolean;
  onClick?: () => unknown;
};

export function Button({ variant = "outline", size = "sm", busy, onClick, className, children, disabled, ...rest }: ButtonProps) {
  const [working, setWorking] = useState(false);
  const live = useRef(true);
  useEffect(() => () => void (live.current = false), []);
  const pending = busy || working;
  return (
    <button
      type="button"
      {...rest}
      disabled={disabled || pending}
      aria-busy={pending || undefined}
      onClick={() => {
        const r = onClick?.();
        if (r instanceof Promise) {
          setWorking(true);
          r.finally(() => live.current && setWorking(false));
        }
      }}
      className={cls(
        "label inline-flex shrink-0 items-center justify-center gap-2 whitespace-nowrap rounded-sm transition-colors disabled:pointer-events-none disabled:opacity-50 [&_svg]:shrink-0",
        VARIANT[variant],
        SIZE[size],
        className,
      )}
    >
      {pending && <Loader2 className="animate-spin" aria-hidden />}
      {children}
    </button>
  );
}

const control = "w-full rounded-sm border bg-background px-3 py-1 text-p-400 transition-colors disabled:cursor-not-allowed disabled:opacity-50";

export const Input = ({ className, ...p }: InputHTMLAttributes<HTMLInputElement>) => <input {...p} className={cls(control, "h-9", className)} />;

function SearchBox({ value, onChange, icon, placeholder = "Search by name..." }: { value: string; onChange: (v: string) => void; icon?: boolean; placeholder?: string }) {
  return (
    <span className="relative w-64">
      {icon && <Search className="pointer-events-none absolute left-3 top-2.5 size-4 text-muted-foreground" aria-hidden />}
      <input
        type="search"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        aria-label={placeholder.replace("...", "")}
        className={cls(control, "text-s-400", icon ? "h-9 pl-10" : "h-[30px]")}
      />
    </span>
  );
}

export const Textarea = ({ className, ...p }: TextareaHTMLAttributes<HTMLTextAreaElement>) => (
  <textarea rows={3} {...p} className={cls(control, "min-h-20 py-2", className)} />
);

export const Select = ({ className, ...p }: SelectHTMLAttributes<HTMLSelectElement>) => (
  <span className={cls("relative block", className)}>
    <select {...p} className={cls(control, "h-9 appearance-none pr-8")} />
    <ChevronDown className="pointer-events-none absolute right-3 top-2.5 size-4 opacity-50" aria-hidden />
  </span>
);

export const Field = ({ label, hint, children }: { label: string; hint?: ReactNode; children: ReactNode }) => (
  <label className="grid gap-1.5">
    <span className="text-s-500 text-secondary">{label}</span>
    {children}
    {hint && <span className="text-s-400 text-muted-foreground">{hint}</span>}
  </label>
);

export const Badge = ({ warn, children }: { warn?: boolean; children: ReactNode }) => (
  <span className={cls("inline-flex shrink-0 items-center rounded-sm px-2 py-0.5 font-mono text-xs-400", warn ? "bg-warning text-warning-foreground" : "bg-muted text-foreground")}>{children}</span>
);

const LAMP = { live: "bg-available", ring: "bg-available animate-blink", free: "border-[1.5px] border-available", hold: "bg-[#e5a00d]", wrap: "bg-sky-500", off: "bg-tertiary", bad: "bg-destructive" };
export type Tone = keyof typeof LAMP;
export const Lamp = ({ tone, color, small }: { tone?: Tone; color?: string; small?: boolean }) => (
  <span aria-hidden className={cls("inline-block shrink-0 rounded-full", small ? "size-1.5" : "size-2", tone && LAMP[tone])} style={color ? { background: color } : undefined} />
);

export const State = ({ tone, color, dim, children }: { tone?: Tone; color?: string; dim?: boolean; children: ReactNode }) => (
  <span className={cls("inline-flex items-center gap-1.5 font-mono text-xs-400", dim && "text-tertiary")}>
    <Lamp small tone={tone} color={color} />
    {children}
  </span>
);

export const Avatar = ({ name }: { name?: string | null }) => (
  <span aria-hidden className="grid size-[30px] shrink-0 place-items-center rounded-full bg-brand text-s-500 text-white">
    {(name?.trim() || "?").charAt(0).toUpperCase()}
  </span>
);

export const Section = ({ title, icon, children }: { title: string; icon?: ReactNode; children: ReactNode }) => (
  <section className="border-b border-st2">
    <h3 className="flex items-center gap-2 p-4 pb-3 text-xxs-600 uppercase [&_svg]:size-4">
      {icon}
      {title}
    </h3>
    <div className="px-4 pb-4">{children}</div>
  </section>
);

export const Facts = ({ rows }: { rows: [string, ReactNode][] }) => (
  <dl className="grid grid-cols-[35%_65%] text-s-400">
    {rows.map(([k, v]) => (
      <div key={k} className="contents">
        <dt className="truncate py-2 pr-3 text-secondary">{k}</dt>
        <dd className="min-w-0 break-words py-2 pl-3">{v}</dd>
      </div>
    ))}
  </dl>
);

export const Page = ({ title, sub, children }: { title: string; sub?: ReactNode; children: ReactNode }) => (
  <div className="flex h-full flex-col px-6 pt-5">
    <h1 className="title">{title}</h1>
    {sub && <p className="text-s-400 text-secondary">{sub}</p>}
    {children}
  </div>
);

export const Card = ({ children }: { children: ReactNode }) => (
  <main className="grid min-h-dvh place-items-center bg-bg2 p-4">
    <div className="w-full max-w-md">
      <p className="mb-6 text-center font-display text-[17px] font-semibold tracking-tight">human-specialist</p>
      <div className="rounded-sm border bg-background p-6 sm:p-8">{children}</div>
    </div>
  </main>
);

export const Empty = ({ title, children }: { title: string; children?: ReactNode }) => (
  <div className="flex flex-col items-center justify-center gap-2 px-6 py-16 text-center text-s-400 text-secondary">
    <p>{title}</p>
    {children && <div className="flex max-w-sm flex-col items-center gap-3">{children}</div>}
  </div>
);

export const Notice = ({ tone = "warning", children }: { tone?: "warning" | "danger"; children: ReactNode }) => (
  <div role="alert" className={cls("flex items-start gap-3 rounded-sm bg-muted px-6 py-3 text-p-400", tone === "danger" && "text-destructive")}>
    <Info className="mt-[3px] size-4 shrink-0" aria-hidden />
    <div>{children}</div>
  </div>
);

export function useLoad<T>(load: () => Promise<T>, deps: unknown[] = []) {
  const [state, setState] = useState<{ data?: T; error?: string; n: number }>({ n: 0 });
  useEffect(() => {
    let live = true;
    setState((s) => ({ data: s.data, n: s.n }));
    load().then(
      (data) => live && setState((s) => ({ data, n: s.n })),
      (e) => live && setState((s) => ({ error: reason(e), n: s.n })),
    );
    return () => void (live = false);
  }, [...deps, state.n]);
  return { ...state, reload: () => setState((s) => ({ ...s, n: s.n + 1 })) };
}

export function usePaged<T>(get: (cursor: string) => Promise<Paged<T>>) {
  const [rows, setRows] = useState<T[]>([]);
  const [cursor, setCursor] = useState("");
  const load = useLoad(async () => {
    const p = await get(cursor);
    setRows((r) => (cursor ? [...r, ...p.data] : p.data));
    return p.next_cursor;
  }, [cursor]);
  const next = load.data;
  return {
    data: load.data === undefined && !cursor ? undefined : rows,
    error: load.error,
    reload: () => (cursor ? setCursor("") : load.reload()),
    more: next ? () => setCursor(`&cursor=${encodeURIComponent(next)}`) : undefined,
  };
}

export function Listed<T>({
  load,
  empty,
  children,
}: {
  load: { data?: T[]; error?: string; reload: () => void };
  empty: ReactNode;
  children: (rows: T[]) => ReactNode;
}) {
  if (load.error)
    return (
      <Empty title={load.error}>
        <Button onClick={load.reload}>Try again</Button>
      </Empty>
    );
  if (!load.data)
    return (
      <div className="grid gap-3 py-4" aria-busy="true" aria-label="Loading">
        {[0, 1, 2].map((i) => (
          <div key={i} className="h-5 animate-pulse rounded-sm bg-muted" style={{ width: `${90 - i * 18}%` }} />
        ))}
      </div>
    );
  return load.data.length ? children(load.data) : empty;
}

type Row = { id: string; at: string; text: string; side?: "inbound" | "outbound"; note?: string };
const BUBBLE = { inbound: "justify-self-start border", outbound: "justify-self-end bg-muted" };
export const Timeline = ({ rows, start }: { rows: Row[]; start?: boolean }) => (
  <ol className={cls("grid justify-items-center py-4", rows.some((r) => r.side) ? "gap-3" : "gap-10")}>
    {start && <li className="rounded-sm bg-muted p-2 font-mono text-xs-400 text-secondary">Start of the conversation</li>}
    {rows.map((r) => (
      <li key={r.id} className={r.side ? cls("max-w-[80%] whitespace-pre-wrap break-words rounded-sm px-3 py-2 text-p-400", BUBBLE[r.side]) : "text-center text-xs-400 text-secondary"}>
        {r.side && <span className="sr-only">{r.side === "inbound" ? "Customer: " : "Agent: "}</span>}
        {r.text}
        <span className={cls(r.side && "block text-xs-400 text-tertiary")}>
          {!r.side && <span aria-hidden> • </span>}
          <time dateTime={r.at}>{time(r.at)}</time>
          {r.note && `, ${r.note}`}
        </span>
      </li>
    ))}
  </ol>
);

export function Tile({ title, when, icon, line, active, href, children }: { title: string; when?: string; icon: ReactNode; line: string; active?: boolean; href?: string; children?: ReactNode }) {
  const Tag = href ? "a" : "div";
  return (
    <Tag href={href} aria-current={active || undefined} className={cls("group relative grid gap-1 rounded-sm p-2 hover:bg-hv1", active && "bg-muted hover:bg-muted")}>
      <span className="flex items-center justify-between gap-2">
        <span className="truncate text-p-400">{title}</span>
        {when && <span className="shrink-0 text-xs-400 text-tertiary">{when}</span>}
      </span>
      <span className="flex items-center gap-1 text-xs-400 text-tertiary [&_svg]:size-4 [&_svg]:shrink-0 [&_svg]:text-brand">
        {icon}
        <span className={cls("truncate", active && "text-primary")}>{line}</span>
      </span>
      {children}
    </Tag>
  );
}

const th = "label-wide h-10 border-b px-2 text-left align-middle text-secondary";
export const td = "h-[45px] px-2 py-2.5 align-middle";
export const tr = "border-b transition-colors hover:bg-muted";

export function Table({ head, children }: { head: [ReactNode, string?][]; children: ReactNode }) {
  return (
    <table className="w-full text-s-400">
      <thead>
        <tr>
          {head.map(([h, extra], i) => (
            <th key={i} className={cls(th, extra)}>
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}

const PAGE = 20;
const pagerButton = "grid size-6 place-items-center rounded-sm border bg-background disabled:opacity-50";

function Pager({ at, count, more, go }: { at: number; count: number; more?: () => void; go: (at: number) => void }) {
  const last = Math.max(0, Math.ceil(count / PAGE) - 1) * PAGE;
  const end = Math.min(at + PAGE, count);
  return (
    <nav aria-label="Pages" className="flex shrink-0 items-center justify-end gap-2 py-3">
      <p className="mr-10 font-mono text-xs-400 text-secondary">
        {count ? at + 1 : 0}-{end} of {count}
        {more ? "+" : ""}
      </p>
      <button type="button" className={pagerButton} aria-label="First page" disabled={!at} onClick={() => go(0)}>
        <ChevronsLeft className="size-3.5" aria-hidden />
      </button>
      <button type="button" className={pagerButton} aria-label="Previous page" disabled={!at} onClick={() => go(at - PAGE)}>
        <ChevronLeft className="size-3.5" aria-hidden />
      </button>
      <button
        type="button"
        className={pagerButton}
        aria-label="Next page"
        disabled={end >= count && !more}
        onClick={() => {
          if (end >= count) more?.();
          go(at + PAGE);
        }}
      >
        <ChevronRight className="size-3.5" aria-hidden />
      </button>
      <button type="button" className={pagerButton} aria-label="Last page" disabled={at >= last} onClick={() => go(last)}>
        <ChevronsRight className="size-3.5" aria-hidden />
      </button>
    </nav>
  );
}

type Col<T> = [head: ReactNode, cell: (row: T) => ReactNode, extra?: string];

export function Reorder<T extends { id: string }>({ rows, cols, open, save, below, bare }: { rows: T[]; cols: Col<T>[]; open?: (row: T) => void; save: (ids: string[]) => Promise<unknown>; below?: (row: T) => ReactNode; bare?: boolean }) {
  const from = useRef(-1);
  const move = (i: number, j: number) => {
    if (i === j || j < 0 || j >= rows.length) return;
    const ids = rows.map((r) => r.id);
    ids.splice(j, 0, ...ids.splice(i, 1));
    act("Saving the order", () => save(ids), "Order saved");
  };
  const grid = { gridTemplateColumns: `repeat(${cols.length}, minmax(0, 1fr))` };
  return (
    <div className="text-s-400">
      {!bare && (
        <div className="label-wide grid gap-x-1 border-b py-2.5 text-secondary" style={{ gridTemplateColumns: `28px ${grid.gridTemplateColumns}` }}>
          {!below && <span />}
          {cols.map(([h], i) => (
            <span key={i} className={cls(below && !i && "col-span-2 pl-2")}>
              {h}
            </span>
          ))}
        </div>
      )}
      {rows.map((r, i) => (
        <div key={r.id} draggable onDragStart={(e) => (e.stopPropagation(), (from.current = i))} onDragOver={(e) => e.preventDefault()} onDrop={(e) => (e.stopPropagation(), move(from.current, i))} className="flex items-center gap-1 border-b hover:bg-bg2">
          <button
            type="button"
            aria-label="Move. Use the up and down arrow keys."
            className="grid w-7 shrink-0 cursor-grab place-items-center self-stretch text-secondary"
            onKeyDown={(e) => (e.key === "ArrowUp" || e.key === "ArrowDown") && (e.preventDefault(), move(i, i + (e.key === "ArrowUp" ? -1 : 1)))}
          >
            <GripVertical className="size-3.5" aria-hidden />
          </button>
          <div className={cls("flex-1", below && "pl-6")}>
            <div className={cls("grid items-center gap-4", below ? "py-4" : bare ? "py-2.5" : "py-3", open && "cursor-pointer")} style={grid} onClick={open && (() => open(r))} onKeyDown={open && ((e) => e.key === "Enter" && open(r))} tabIndex={open ? 0 : undefined}>
              {cols.map(([, cell], c) => (
                <div key={c} className="min-w-0 truncate">
                  {cell(r)}
                </div>
              ))}
            </div>
            {below?.(r)}
          </div>
        </div>
      ))}
    </div>
  );
}

export function ListView<T extends { id: string }>({
  load,
  cols,
  search,
  iconSearch,
  action,
  empty,
  open,
  more,
  placeholder,
  reorder,
  below,
  refresh,
}: {
  load: { data?: T[]; error?: string; reload: () => void };
  refresh?: boolean;
  cols: Col<T>[];
  search?: (row: T) => string | null;
  iconSearch?: boolean;
  placeholder?: string;
  reorder?: (ids: string[]) => Promise<unknown>;
  below?: (row: T) => ReactNode;
  action?: ReactNode;
  empty: ReactNode;
  open?: (row: T) => void;
  more?: () => void;
}) {
  const [q, setQ] = useState("");
  const [page, setPage] = useState(0);
  const bare = (s: string) => s.toLowerCase().replace(/[\s+()#-]/g, "");
  const needle = bare(q);
  const rows = needle && search ? load.data?.filter((r) => bare(search(r) ?? "").includes(needle)) : load.data;
  const at = Math.min(page, Math.max(0, Math.ceil((rows?.length ?? 0) / PAGE) - 1) * PAGE);
  return (
    <>
      <div className={cls("flex items-center justify-between gap-3", iconSearch ? "mt-4" : "mt-6")}>
        {search ? (
          <SearchBox
            icon={iconSearch}
            placeholder={placeholder}
            value={q}
            onChange={(v) => {
              setQ(v);
              setPage(0);
            }}
          />
        ) : (
          <span />
        )}
        <span className="flex items-center gap-2">
          {refresh && (
            <Button aria-label="Refresh" onClick={load.reload}>
              <RefreshCcw aria-hidden />
            </Button>
          )}
          {action}
        </span>
      </div>
      <div className="mt-3 min-h-0 flex-1 overflow-auto">
        <Listed load={{ ...load, data: rows }} empty={needle ? <Empty title="No results." /> : empty}>
          {(list) =>
            reorder ? (
              <Reorder rows={list} cols={cols} open={open} save={reorder} below={below} />
            ) : (
              <Table head={cols.map(([h, , extra]) => [h, extra])}>
                {list.slice(at, at + PAGE).map((r) => (
                  <tr
                    key={r.id}
                    className={cls(tr, open && "cursor-pointer")}
                    onClick={open && (() => open(r))}
                    onKeyDown={open && ((e) => e.key === "Enter" && open(r))}
                    tabIndex={open ? 0 : undefined}
                  >
                    {cols.map(([, cell, extra], i) => (
                      <td key={i} className={cls(td, i === 0 && "font-medium", extra)}>
                        {cell(r)}
                      </td>
                    ))}
                  </tr>
                ))}
              </Table>
            )
          }
        </Listed>
      </div>
      {rows && !reorder && (rows.length > PAGE || more) && <Pager at={at} count={rows.length} more={more} go={setPage} />}
    </>
  );
}

export function Copy({ value }: { value: string }) {
  const [done, setDone] = useState(false);
  return (
    <Button
      onClick={() =>
        act("Copying", async () => {
          await navigator.clipboard.writeText(value);
          setDone(true);
          setTimeout(() => setDone(false), 2000);
        }, "Copied to the clipboard")
      }
    >
      {done ? "Copied" : "Copy"}
    </Button>
  );
}

type BoxProps = { title: ReactNode; sub?: ReactNode; open: boolean; onClose: () => void; children: ReactNode };

function Box({ title, sub, open, onClose, className, children }: BoxProps & { className: string }) {
  return (
    <Dialog.Root open={open} onOpenChange={(o) => !o && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-black/70" />
        <Dialog.Content
          className={cls("fixed z-50 flex flex-col rounded-sm border bg-background py-6 shadow-lg", className)}
          onInteractOutside={(e) => (e.target as Element).closest?.("[data-toast]") && e.preventDefault()}
        >
          <div className="flex items-start justify-between gap-2 px-6">
            <div>
              <Dialog.Title className="title">{title}</Dialog.Title>
              <Dialog.Description className="text-s-400 text-secondary">{sub}</Dialog.Description>
            </div>
            <Dialog.Close className="mt-2 rounded-sm opacity-70 hover:opacity-100" aria-label="Close">
              <X className="size-4" />
            </Dialog.Close>
          </div>
          {children}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

export const Sheet = ({ children, ...p }: BoxProps) => (
  <Box {...p} className="inset-y-0 right-0 w-full gap-2 sm:inset-y-4 sm:right-4 sm:w-[550px]">
    <div className="min-h-0 flex-1 overflow-y-auto px-2">{children}</div>
  </Box>
);

export const Modal = ({ children, ...p }: BoxProps) => (
  <Box {...p} className="left-1/2 top-1/2 max-h-[90vh] w-[calc(100%-2rem)] max-w-lg -translate-x-1/2 -translate-y-1/2 gap-5 overflow-y-auto">
    <div className="px-6">{children}</div>
  </Box>
);

type EditProps<B> = {
  what: string;
  row?: { id: string };
  body: (f: FormData) => B;
  save: (body: B) => Promise<unknown>;
  remove?: () => Promise<unknown>;
  warn?: (body: B) => [notice: string, yes: string] | undefined;
  left?: ReactNode;
  reload: () => void;
  onClose: () => void;
  children: ReactNode;
};

export function Confirm({ label, sure = "Press again to delete for good", run }: { label: string; sure?: string; run: () => Promise<unknown> }) {
  const [armed, setArmed] = useState(false);
  return (
    <Button variant="danger" size="md" onBlur={() => setArmed(false)} onClick={() => (armed ? run() : setArmed(true))}>
      {armed ? sure : label}
    </Button>
  );
}

export const Editor = <B,>(p: EditProps<B>) => (
  <Sheet open onClose={p.onClose} title={`${p.row ? "Edit" : "Create"} ${p.what.replace(/\b\w/g, (c) => c.toUpperCase())}`}>
    <EditForm {...p} className="min-h-full grid-rows-[1fr_auto] px-4 py-2" />
  </Sheet>
);

export function EditForm<B>({ what, row, body, save, remove, warn, left, reload, onClose, children, className }: EditProps<B> & { className?: string }) {
  const [armed, setArmed] = useState<[string, string]>();
  const done = (r: unknown) => {
    if (!r) return;
    reload();
    onClose();
  };
  return (
    <Form
      changes
      className={className}
      onSubmit={async (f) => {
        const b = body(f);
        const w = warn?.(b);
        if (w && w[0] !== armed?.[0]) return setArmed(w);
        done(await act(row ? `Saving the ${what}` : `Adding the ${what}`, () => save(b), `${cap(what)} ${row ? "saved" : "added"}`));
      }}
    >
      <div className="grid content-start gap-5">
        {children}
        {armed && <Notice>{armed[0]}</Notice>}
      </div>
      <div className="flex items-center justify-between gap-2">
        <span className="flex flex-wrap gap-2">
          {left}
          {remove && <Confirm label={`Delete ${what}`} run={async () => done(await act(`Deleting the ${what}`, remove, `${cap(what)} deleted`))} />}
        </span>
        <Submit>{armed?.[1] ?? `${row ? "Update" : "Create"} ${what}`}</Submit>
      </div>
    </Form>
  );
}

export function Rows<T>({ label, items, blank, add, children }: { label: string; items: T[]; blank: T; add: string; children: (item: T) => ReactNode }) {
  const [rows, setRows] = useState(() => items.map((item, key) => ({ key, item })));
  const next = useRef(items.length);
  return (
    <fieldset className="grid gap-2">
      <legend className="pb-1.5 text-s-500 text-secondary">{label}</legend>
      {rows.map(({ key, item }) => (
        <div key={key} className="flex items-center gap-2">
          {children(item)}
          <button type="button" aria-label="Remove" className="grid size-8 shrink-0 place-items-center rounded-sm hover:bg-muted" onClick={() => setRows(rows.filter((r) => r.key !== key))}>
            <X className="size-4" aria-hidden />
          </button>
        </div>
      ))}
      <Button className="justify-self-start" onClick={() => setRows([...rows, { key: next.current++, item: blank }])}>
        <Plus aria-hidden /> {add}
      </Button>
    </fieldset>
  );
}

export const Toggle = ({ label, ...p }: InputHTMLAttributes<HTMLInputElement> & { label: string }) => (
  <label className="flex items-center gap-2 text-p-500">
    <input type="checkbox" role="switch" {...p} className="peer sr-only" />
    <span aria-hidden className="relative h-4 w-7 shrink-0 rounded-full bg-st2 transition-colors peer-checked:bg-brand peer-focus-visible:outline-2 peer-focus-visible:outline-ring after:absolute after:left-0.5 after:top-0.5 after:size-3 after:rounded-full after:bg-white after:transition-transform peer-checked:after:translate-x-3" />
    {label}
  </label>
);

export const Tabs = (p: ComponentProps<typeof RTabs.Root>) => <RTabs.Root {...p} onValueChange={(v) => (clearErrors(), p.onValueChange?.(v))} />;
export const TabPanel = RTabs.Content;
export const TabList = ({ children, label }: { children: ReactNode; label: string }) => (
  <RTabs.List aria-label={label} className="flex w-full items-center gap-4 border-b">
    {children}
  </RTabs.List>
);
export const Tab = ({ value, children }: { value: string; children: ReactNode }) => (
  <RTabs.Trigger
    value={value}
    className="-mb-px border-b-2 border-transparent p-2 text-s-500 text-muted-foreground transition-colors hover:text-foreground data-[state=active]:border-primary data-[state=active]:text-foreground"
  >
    {children}
  </RTabs.Trigger>
);

const Pending = createContext({ busy: false, idle: false });
export function Form({ onSubmit, children, className, changes }: { onSubmit: (data: FormData, form: HTMLFormElement) => Promise<unknown>; children: ReactNode; className?: string; changes?: boolean }) {
  const [busy, setBusy] = useState(false);
  const [dirty, setDirty] = useState(false);
  return (
    <form
      className={cls("grid gap-5", className)}
      onChange={() => setDirty(true)}
      onSubmit={(e: FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        const form = e.currentTarget;
        setBusy(true);
        onSubmit(new FormData(form), form).finally(() => setBusy(false));
      }}
    >
      <Pending.Provider value={{ busy, idle: !!changes && !dirty }}>{children}</Pending.Provider>
    </form>
  );
}
export function Submit(p: Omit<ButtonProps, "type" | "busy">) {
  const f = useContext(Pending);
  return <Button variant="primary" size="md" {...p} disabled={p.disabled || f.idle} type="submit" busy={f.busy} />;
}

type Toast = { id: number; kind: "busy" | "ok" | "error"; text: string };
let toasts: Toast[] = [];
let seq = 0;
const listeners = new Set<() => void>();
const publish = (next: Toast[]) => {
  toasts = next;
  listeners.forEach((l) => l());
};
const drop = (id: number) => publish(toasts.filter((t) => t.id !== id));
const clearErrors = () => publish(toasts.filter((t) => t.kind !== "error"));

export const failed = (label: string, e: unknown) => publish([...toasts.slice(-2), { id: ++seq, kind: "error", text: `${label} failed. ${reason(e)}` }]);

export async function act<T>(label: string, run: () => Promise<T>, done: string | ((r: T) => string)): Promise<T | undefined> {
  const id = ++seq;
  publish([...toasts.filter((t) => t.kind !== "error").slice(-2), { id, kind: "busy", text: `${label}…` }]);
  try {
    const r = await run();
    publish(toasts.map((t) => (t.id === id ? { id, kind: "ok", text: typeof done === "function" ? done(r) : done } : t)));
    setTimeout(() => drop(id), 4000);
    return r ?? (true as T);
  } catch (e) {
    publish(toasts.map((t) => (t.id === id ? { id, kind: "error", text: `${label} failed. ${reason(e)}` } : t)));
    return undefined;
  }
}

export function Toaster() {
  const list = useSyncExternalStore(
    (l) => (listeners.add(l), () => void listeners.delete(l)),
    () => toasts,
  );
  return (
    <div aria-live="polite" className="pointer-events-none fixed bottom-4 left-4 z-[60] flex w-[calc(100%-2rem)] max-w-sm flex-col gap-2">
      {list.map((t) => (
        <div
          key={t.id}
          data-toast
          role={t.kind === "error" ? "alert" : undefined}
          className="pointer-events-auto flex items-start gap-2 rounded-sm border bg-background px-4 py-3 text-p-400 shadow-sh1 [&_svg]:mt-1 [&_svg]:size-4 [&_svg]:shrink-0"
        >
          {t.kind === "busy" ? <Loader2 className="animate-spin" aria-hidden /> : t.kind === "ok" ? <CircleCheck className="text-available" aria-hidden /> : <CircleAlert className="text-destructive" aria-hidden />}
          <span className="flex-1">{t.text}</span>
          {t.kind === "error" && (
            <button type="button" aria-label="Dismiss" className="rounded-sm opacity-70 hover:opacity-100" onClick={() => drop(t.id)}>
              <X />
            </button>
          )}
        </div>
      ))}
    </div>
  );
}
