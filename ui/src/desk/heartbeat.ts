// HS signs an agent in on a session's second beat, so the desk sends two at once. The desk's heartbeat runs in a worker: a hidden tab slows the page's own timers to once a minute, past HS's 40 s limit.
let timer: ReturnType<typeof setInterval>;
onmessage = ({ data: [method, path, base, token] }: MessageEvent<[string, string, string, string]>) => {
  clearInterval(timer);
  const beat = () =>
    fetch(base + path, { method, headers: { Authorization: `Bearer ${token}` } }).then(
      (r) => postMessage(r.status),
      () => postMessage(0),
    );
  beat().then(beat);
  timer = setInterval(beat, 15000);
};
