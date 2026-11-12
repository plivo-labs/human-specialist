import * as Dialog from "@radix-ui/react-dialog";
import * as Menu from "@radix-ui/react-dropdown-menu";
import { type LucideIcon, Moon, PanelLeft, Sun } from "lucide-react";
import { type ReactNode, useState, useSyncExternalStore } from "react";
import { signOut } from "./api";
import { Avatar, cls } from "./ui";

const onHash = (l: () => void) => (addEventListener("hashchange", l), () => removeEventListener("hashchange", l));
export const useRoute = () => useSyncExternalStore(onHash, () => location.hash.replace(/^#/, "") || "/");
export const go = (route: string) => (location.hash = route);

const dark = () => document.documentElement.classList.contains("dark");
export function initTheme() {
  const saved = localStorage.getItem("hs.theme");
  document.documentElement.classList.toggle("dark", saved ? saved === "dark" : matchMedia("(prefers-color-scheme: dark)").matches);
}

export type NavItem = { route: string; label: string; icon: LucideIcon; group?: string };

function Nav({ items, route, onPick }: { items: NavItem[]; route: string; onPick?: () => void }) {
  const groups = [...new Set(items.map((i) => i.group ?? ""))];
  return (
    <nav className="flex-1 overflow-y-auto" aria-label="Main">
      {groups.map((g) => (
        <div key={g} className="px-3 py-2">
          {g && <p className="label-wide mb-1.5 mt-3 px-2 text-muted-foreground">{g}</p>}
          <div className="flex flex-col gap-1">
            {items
              .filter((i) => (i.group ?? "") === g)
              .map(({ route: r, label, icon: Icon }) => (
                <a
                  key={r}
                  href={`#${r}`}
                  onClick={onPick}
                  aria-current={route === r || route.startsWith(`${r}/`) ? "page" : undefined}
                  className="flex h-8 items-center gap-2 rounded-sm p-2 text-s-400 text-nav hover:bg-muted aria-[current=page]:bg-muted aria-[current=page]:font-medium aria-[current=page]:text-nav-active"
                >
                  <Icon className="size-3.5 shrink-0" aria-hidden />
                  <span className="truncate">{label}</span>
                </a>
              ))}
          </div>
        </div>
      ))}
    </nav>
  );
}

const Wordmark = () => <div className="flex h-14 shrink-0 items-center px-6 font-display text-[17px] font-semibold tracking-tight">human-specialist</div>;

export function Shell({ nav, route, org, status, user, children }: { nav: NavItem[]; route: string; org?: string | null; status?: ReactNode; user: ReactNode; children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="flex h-dvh overflow-hidden">
      <aside className="hidden w-60 shrink-0 flex-col border-r border-st2 md:flex">
        <Wordmark />
        <Nav items={nav} route={route} />
      </aside>
      <Dialog.Root open={open} onOpenChange={setOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-40 bg-black/70" />
          <Dialog.Content className="fixed inset-y-0 left-0 z-50 flex w-60 flex-col border-r border-st2 bg-background">
            <Dialog.Title className="sr-only">Menu</Dialog.Title>
            <Dialog.Description />
            <Wordmark />
            <Nav items={nav} route={route} onPick={() => setOpen(false)} />
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-2 border-b px-4">
          <button type="button" className="grid size-[30px] place-items-center rounded-sm hover:bg-muted md:hidden" aria-label="Open the menu" onClick={() => setOpen(true)}>
            <PanelLeft className="size-4" aria-hidden />
          </button>
          {org && <span className="truncate px-2 text-s-500">{org}</span>}
          <div className="ml-auto flex items-center gap-2">
            {status}
            <ThemeToggle />
            {user}
          </div>
        </header>
        <main className="min-h-0 flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}

function ThemeToggle() {
  const [, flip] = useState(0);
  const toggle = () => {
    const next = !dark();
    document.documentElement.classList.toggle("dark", next);
    localStorage.setItem("hs.theme", next ? "dark" : "light");
    flip((n) => n + 1);
  };
  return (
    <button type="button" onClick={toggle} aria-label={dark() ? "Use the light theme" : "Use the dark theme"} className="grid size-[30px] place-items-center rounded-sm hover:bg-muted">
      {dark() ? <Sun className="size-4" aria-hidden /> : <Moon className="size-4" aria-hidden />}
    </button>
  );
}

export const menuItem = "flex h-[34px] cursor-default select-none items-center gap-2 rounded-sm px-2 text-s-400 outline-none data-[highlighted]:bg-muted";

export function Dropdown({ trigger, children, label, className, onOpen }: { trigger: ReactNode; children: ReactNode; label: string; className?: string; onOpen?: () => void }) {
  return (
    <Menu.Root onOpenChange={(o) => o && onOpen?.()}>
      <Menu.Trigger asChild aria-label={label}>
        {trigger}
      </Menu.Trigger>
      <Menu.Portal>
        <Menu.Content align="start" sideOffset={4} className={cls("z-50 min-w-48 overflow-y-auto rounded-sm border bg-background p-1 shadow-md", className)}>
          {children}
        </Menu.Content>
      </Menu.Portal>
    </Menu.Root>
  );
}

export function UserMenu({ name, sub, onPassword }: { name: string; sub: string; onPassword?: () => void }) {
  return (
    <Dropdown
      label="Account"
      trigger={
        <button type="button" className="rounded-full">
          <Avatar name={name} />
        </button>
      }
    >
      <div className="px-2 py-1.5">
        <p className="text-s-600">{name}</p>
        <p className="text-xs-400 text-secondary">{sub}</p>
      </div>
      <Menu.Separator className="my-1 h-px bg-border" />
      {onPassword && (
        <Menu.Item className={menuItem} onSelect={onPassword}>
          Change password
        </Menu.Item>
      )}
      <Menu.Item className={cls(menuItem, "text-destructive")} onSelect={signOut}>
        Sign out
      </Menu.Item>
    </Dropdown>
  );
}
