import { Briefcase, Calendar, ChartPie, Clock, Code, Contact, Hash, Inbox, ListTodo, MessageSquare, PhoneOutgoing, UserCog, Users, UsersRound } from "lucide-react";
import { type ReactNode, useState } from "react";
import { createRoot } from "react-dom/client";
import { held, hold, store } from "./api";
import { Conversations } from "./conversations";
import { type Ctx, Desk, Desked, Dialer, StatusMenu } from "./desk/desk";
import { Floor } from "./desk/floor";
import { DeskPage } from "./desk/side";
import { time } from "./format";
import { type Need, holds, landing, visible } from "./held";
import "./index.css";
import { Agents, ROLES } from "./setup/agents";
import { Contacts, Hours, Skills, Statuses, Tasks, Teams } from "./setup/lists";
import { Outbound, Workload } from "./setup/settings";
import { Numbers, Queues } from "./setup/routing";
import { Dispositions, PlivoAccount } from "./setup/tenant";
import { type NavItem, Shell, UserMenu, go, initTheme, useRoute } from "./shell";
import { ChangePassword, SignIn, You } from "./signin";
import { Button, Toaster } from "./ui";

function pickUpSession(e?: Event) {
  const handed = new URLSearchParams(location.hash.slice(1)).get("session");
  if (!handed) return;
  // A desk link is one agent's identity: a key the tab inherited from its opener must not ride along.
  hold({ key: null, session: JSON.parse(atob(handed)) });
  history.replaceState(null, "", "#/desk");
  if (e) location.reload();
}

const NAV: (NavItem & { need?: Need; page: (id?: string, ctx?: Ctx) => ReactNode })[] = [
  { route: "/desk", label: "My Inbox", icon: Inbox, need: "session", page: (id, ctx) => <DeskPage ctx={ctx!} id={id} /> },
  { route: "/floor", label: "Floor", icon: ChartPie, group: "Monitor", need: "boss", page: (_, ctx) => <Floor ctx={ctx!} /> },
  { route: "/conversations", label: "Conversations", icon: MessageSquare, page: (id) => <Conversations id={id} /> },
  { route: "/tasks", label: "Tasks", icon: ListTodo, page: () => <Tasks /> },
  { route: "/teams", label: "Teams", icon: UsersRound, group: "Settings", page: () => <Teams /> },
  { route: "/queues", label: "Queues", icon: Users, group: "Settings", page: () => <Queues /> },
  { route: "/skills", label: "Skills", icon: Briefcase, group: "Settings", page: () => <Skills /> },
  { route: "/statuses", label: "Availability Status", icon: Clock, group: "Settings", page: () => <Statuses /> },
  { route: "/contacts", label: "Internal Contacts", icon: Contact, group: "Settings", page: () => <Contacts /> },
  { route: "/outbound", label: "Outbound Configurations", icon: PhoneOutgoing, group: "Settings", page: () => <Outbound /> },
  { route: "/hours", label: "Business Hours & Holidays", icon: Calendar, group: "Settings", page: () => <Hours /> },
  { route: "/workload", label: "Workload", icon: UserCog, group: "Settings", page: () => <Workload /> },
  { route: "/dispositions", label: "Conversation Configurations", icon: MessageSquare, group: "Settings", page: () => <Dispositions /> },
  { route: "/agents", label: "Users", icon: Users, group: "Settings", page: () => <Agents /> },
  { route: "/numbers", label: "Phone Numbers", icon: Hash, group: "Connect", page: () => <Numbers /> },
  { route: "/plivo", label: "Plivo Account", icon: Code, group: "Connect", page: () => <PlivoAccount /> },
];

function Frame({ route, ctx }: { route: string; ctx?: Ctx }) {
  const me = ctx?.s.me;
  const h = holds(!!held().key, me && me.role);
  const nav = visible(NAV, h);
  const [, section, id] = route.split("/");
  const here = nav.find((n) => n.route === `/${section}`) ?? nav.find((n) => n.route === landing(h))!;
  const org = store.get<string>("tenantName");
  const until = held().session?.renewable_until;
  const [changing, setChanging] = useState(false);
  return (
    <Shell
      nav={nav}
      route={here.route}
      org={org}
      status={ctx ? <><Dialer ctx={ctx} /><StatusMenu ctx={ctx} /></> : <Button onClick={() => go("/you")}>Take calls</Button>}
      user={
        <>
          <UserMenu name={me?.name ?? org ?? "Organization"} sub={me ? `${ROLES[me.role ?? "agent"]}.${until ? ` Signed in until ${time(until)}.` : ""}` : "Signed in with the organization key"} onPassword={me && (() => setChanging(true))} />
          {changing && <ChangePassword onClose={() => setChanging(false)} />}
        </>
      }
    >
      {ctx ? <Desked ctx={ctx}>{here.page(id, ctx)}</Desked> : here.page(id)}
    </Shell>
  );
}

function App() {
  const route = useRoute().split("?")[0];
  const { key, session } = held();
  if (route === "/signin" || (!key && !session)) return <SignIn />;
  if (session) return <Desk session={session}>{(ctx) => <Frame route={route} ctx={ctx} />}</Desk>;
  return route === "/you" ? <You /> : <Frame route={route} />;
}

initTheme();
pickUpSession();
addEventListener("hashchange", pickUpSession);
createRoot(document.getElementById("root")!).render(
  <>
    <App />
    <Toaster />
  </>,
);
