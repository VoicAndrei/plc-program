"""
OPC UA sim server. Reads tags.yaml and exposes each tag as a Float variable
under a shared namespace. A per-tag generator produces realistic waveforms
at the configured sample interval so the downstream logger + dashboard have
something to chew on while no real PLC is connected.
"""
import asyncio
import math
import random
from pathlib import Path

import yaml
from asyncua import Server, ua

HERE = Path(__file__).parent
CONN_CFG = HERE / "connection.yaml"
TAGS_CFG = HERE / "tags.yaml"


class Generator:
    """Stateful signal generator. One per tag."""

    def __init__(self, spec: dict, group_state: dict):
        self.spec = spec
        self.group_state = group_state
        self.t = 0.0
        self.state = {}
        p = spec["pattern"]
        lo, hi = spec["min"], spec["max"]
        avg = spec.get("avg", (lo + hi) / 2)
        if p == "drift":
            self.state["phase"] = random.uniform(0, math.tau)
            self.state["period"] = random.uniform(180, 900)
            self.state["amp"] = min(avg - lo, hi - avg) * 0.6
        elif p == "stepped":
            self.state["value"] = avg
            self.state["hold_until"] = 0.0
        elif p == "burst":
            self.state["value"] = lo
            self.state["burst_until"] = 0.0
            self.state["idle_until"] = random.uniform(10, 60)
            self.state["peak"] = hi
        elif p == "ramp":
            self.state["value"] = lo
            self.state["dir"] = 1
            self.state["rate"] = (hi - lo) / random.uniform(600, 1800)
        elif p == "noisy":
            self.state["sigma"] = (hi - lo) * 0.03
        elif p == "motor":
            self.state["scale"] = (hi - lo)

    def tick(self, dt: float) -> float:
        self.t += dt
        p = self.spec["pattern"]
        lo, hi = self.spec["min"], self.spec["max"]
        avg = self.spec.get("avg", (lo + hi) / 2)

        if p == "drift":
            s = self.state
            sine = math.sin(self.t / s["period"] * math.tau + s["phase"])
            noise = random.gauss(0, s["amp"] * 0.08)
            return _clamp(avg + sine * s["amp"] + noise, lo, hi)

        if p == "stepped":
            s = self.state
            if self.t >= s["hold_until"]:
                s["value"] = random.uniform(lo, hi)
                s["hold_until"] = self.t + random.uniform(30, 120)
            return s["value"] + random.gauss(0, (hi - lo) * 0.002)

        if p == "burst":
            s = self.state
            if self.t < s["idle_until"]:
                return _clamp(lo + random.gauss(0, (hi - lo) * 0.003), lo, hi)
            if self.t < s["burst_until"]:
                return _clamp(s["peak"] + random.gauss(0, (hi - lo) * 0.02), lo, hi)
            # Transition: schedule next burst
            if random.random() < 0.5:
                s["peak"] = random.uniform((lo + hi) / 2, hi)
                s["burst_until"] = self.t + random.uniform(5, 30)
            else:
                s["idle_until"] = self.t + random.uniform(60, 300)
            return _clamp(lo + random.gauss(0, (hi - lo) * 0.003), lo, hi)

        if p == "ramp":
            s = self.state
            s["value"] += s["dir"] * s["rate"] * dt
            if s["value"] >= hi:
                s["value"] = hi
                s["dir"] = -1
            elif s["value"] <= lo:
                s["value"] = lo
                s["dir"] = 1
            return s["value"] + random.gauss(0, (hi - lo) * 0.002)

        if p == "noisy":
            return _clamp(avg + random.gauss(0, self.state["sigma"]), lo, hi)

        if p == "motor":
            gs = self.group_state
            if self.t >= gs["next_switch"]:
                gs["on"] = not gs["on"]
                gs["next_switch"] = self.t + (
                    random.uniform(60, 240) if gs["on"] else random.uniform(30, 180)
                )
            target = hi * 0.9 if gs["on"] else lo
            # First-order approach to target
            cur = self.state.get("cur", lo)
            cur += (target - cur) * min(1.0, dt / 4.0)
            # Vibration rides higher with variance while on
            jitter = (hi - lo) * (0.05 if gs["on"] else 0.01)
            self.state["cur"] = cur
            return _clamp(cur + random.gauss(0, jitter), lo, hi)

        return avg


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


async def main():
    conn = yaml.safe_load(CONN_CFG.read_text())
    tags_cfg = yaml.safe_load(TAGS_CFG.read_text())
    cfg = {**conn, "tags": tags_cfg["tags"]}
    endpoint = conn["endpoint"]
    namespace = conn.get("sim", {}).get("namespace", "https://plc-program.local/sim")
    dt = float(conn.get("sample_interval_s", 1.0))

    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    server.set_server_name("plc-program sim")
    idx = await server.register_namespace(namespace)

    root = await server.nodes.objects.add_folder(idx, "Plant")

    # Group containers (e.g. "mcc01") so browse tree looks plausible.
    folders = {}

    # One shared state dict per group (motor pattern uses this).
    groups: dict[str, dict] = {}

    variables = []  # list of (tag_spec, ua_node, generator)
    for tag in cfg["tags"]:
        folder_name = tag.get("group") or tag["category"]
        folder = folders.get(folder_name)
        if folder is None:
            folder = await root.add_folder(idx, folder_name)
            folders[folder_name] = folder

        g = tag.get("group")
        if g and g not in groups:
            groups[g] = {"on": False, "next_switch": random.uniform(5, 30)}
        group_state = groups.get(g, {})

        var = await folder.add_variable(
            tag["node"],  # full nodeid string
            tag["name"],
            0.0,
            varianttype=ua.VariantType.Double,
        )
        await var.set_writable()
        variables.append((tag, var, Generator(tag, group_state)))

    print(f"[sim] endpoint {endpoint}  tags={len(variables)}  dt={dt}s")

    async with server:
        while True:
            for tag, var, gen in variables:
                val = gen.tick(dt)
                await var.write_value(ua.Variant(float(val), ua.VariantType.Double))
            await asyncio.sleep(dt)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
