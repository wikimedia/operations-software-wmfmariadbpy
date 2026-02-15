#!/usr/bin/env python3
"""
Plans which replicas to be used as candidates to minimize risk in case of rack failure.

"""

import z3
from pathlib import Path
from pydantic import BaseModel
from collections import Counter

MASTER = 2
CANDIDATE = 1
REPLICA = 0
roles = ["replica", "candidate", "master"]


def optimize_locations(servers, racks, sections) -> dict:
    import time

    opt = z3.Optimize()

    # opt.set("maxsat_engine", "wmax")  # or 'maxres', 'pd-maxres', 'rc2'
    # opt.set("optsmt_engine", "farkas")  # or 'basic', 'symba'
    opt.set("maxsat_engine", "wmax")
    opt.set("timeout", 4 * 60 * 000)

    initial_section_counts = Counter(s.section for s in servers)

    role = []
    sec = []

    # # Mandatory hard constraints # #

    for i, server in enumerate(servers):
        role.append(z3.Int(f"role_{server.hn}"))
        sec.append(z3.Int(f"section_{server.hn}"))

        # Role must be 0, 1, or 2
        opt.add(role[i] >= 0)
        opt.add(role[i] <= 2)

        # Section must be valid
        opt.add(sec[i] >= 0)
        opt.add(sec[i] < len(sections))

    print(f"Constraints so far: {len(opt.assertions())}")

    print("Constraint: Each section has exactly one master")
    for s_idx, section in enumerate(sections):
        masters_in_section = [z3.And(sec[i] == s_idx, role[i] == MASTER) for i in range(len(servers))]
        opt.add(z3.PbEq([(m, 1) for m in masters_in_section], 1))

    print(f"Constraints so far: {len(opt.assertions())}")

    print("Constraint: Total servers per section stays the same")
    for s_idx, section in enumerate(sections):
        servers_in_section = []
        for i in range(len(servers)):
            r = z3.If(sec[i] == s_idx, 1, 0)
            servers_in_section.append(r)

        opt.add(z3.Sum(servers_in_section) == initial_section_counts[section])

    print(f"Constraints so far: {len(opt.assertions())}")

    # # Customizable hard and soft constraints # #

    print("Constraint: Each section has 1-2 candidates")
    for s_idx, section in enumerate(sections):
        candidates_in_section = [z3.And(sec[i] == s_idx, role[i] == CANDIDATE) for i in range(len(servers))]

        # At least one candidate
        # opt.add(z3.PbGe([(c, 1) for c in candidates_in_section], 1))
        # At least two candidates
        opt.add(z3.PbGe([(c, 1) for c in candidates_in_section], 2))

        # if possible, at least 2 cands per section
        # opt.add_soft(z3.PbGe([(c, 1) for c in candidates_in_section], 2), weight=1)

    print(f"Constraints so far: {len(opt.assertions())}")

    print("Soft constraint: Minimize changes")
    for i, server in enumerate(servers):

        # Prefer keeping same section: changing requires cloning
        # opt.add_soft(sec[i] == server.section_idx, weight=3)

        # Force keeping same section: changing requires cloning
        opt.add(sec[i] == server.section_idx)

        # Prefer keeping same role
        if server.role == "master":
            # Changing from/to requires flip
            opt.add_soft(role[i] == 2, weight=2)
        else:
            # Changing candidate<->replica is cheap
            opt.add_soft(role[i] == server.role_idx, weight=1)

    print(f"Constraints so far: {len(opt.assertions())}")

    print("Adding rack based constraints")
    for rack in racks:
        servers_in_rack = [i for i, s in enumerate(servers) if s.rack == rack]
        if len(servers_in_rack) < 2:
            continue

        # Count masters and candidates in this rack
        masters = [z3.If(role[i] == MASTER, 1, 0) for i in servers_in_rack]
        masters_plus_candidates = [z3.If(role[i] != REPLICA, 1, 0) for i in servers_in_rack]

        # Hard no more than 2 masters per rack
        opt.add(z3.Sum(masters) <= 2)
        opt.add_soft(z3.Sum(masters) <= 1, weight=8)

        opt.add(z3.Sum(masters_plus_candidates) <= 2)
        # opt.add_soft(z3.Sum(masters_plus_candidates) <= 2, weight=8)

        # Less strongly penalize rack with both master(s) AND candidate(s)
        # has_master = z3.Sum(masters) >= 1
        # has_candidate = z3.Sum(candidates) >= 1
        # opt.add_soft(z3.Not(z3.And(has_master, has_candidate)), weight=3)

    print(f"Constraints so far: {len(opt.assertions())}")

    assert len(role) == len(servers)
    assert len(sec) == len(servers)

    print("Starting solve...")
    t = time.time()
    result = opt.check()
    print(f"SOLVE TIME: {time.time() - t:.2f}s")

    if result != z3.sat:
        raise RuntimeError(f"No solution: {result}")

    model = opt.model()

    # Extract solution
    assignment = {}
    changes = []

    role_names = ["replica", "candidate", "master"]

    for i, server in enumerate(servers):
        new_role_val = model.evaluate(role[i]).as_long()
        new_section_idx = model.evaluate(sec[i]).as_long()

        new_role = role_names[new_role_val]
        new_section = sections[new_section_idx]

        assignment[server.hn] = {"role": new_role, "section": new_section, "rack": server.rack}

        if new_role != server.role or new_section != server.section:
            changes.append(
                {
                    "server": server.hn,
                    "from_role": server.role,
                    "to_role": new_role,
                    "from_section": server.section,
                    "to_section": new_section,
                }
            )

    return {"changes": changes, "assignment": assignment}


class Srv(BaseModel):
    hn: str
    section: str
    section_idx: int
    role: str
    role_idx: int
    rack: str
    rack_idx: int


def main() -> None:
    servers = []

    # TODO: load from zarcillo
    candidates = """
        db1160
        db1162
        db1173
        db1181
        db1184
        db1189
        db1193
        db1220
        db1230
        db1258
        es1037
        es1039
    """.strip().split()

    # TODO: load from Zarcillo
    for li in Path("data/racks").read_text().splitlines():
        hn, sec, role, rack = li.replace("|", "").split()
        if sec.startswith(("es", "pc", "ms")):
            continue
        if role == "rep":
            if hn in candidates:
                role = "candidate"
            else:
                role = "replica"
        role_idx = roles.index(role)
        servers.append(Srv(hn=hn, section=sec, role=role, rack=rack, role_idx=role_idx, section_idx=0, rack_idx=0))

    racks = sorted(set(s.rack for s in servers))
    sections = sorted(set(s.section for s in servers))
    for s in servers:
        s.rack_idx = racks.index(s.rack)
        s.section_idx = sections.index(s.section)

    result = optimize_locations(servers, racks, sections)

    if result["changes"]:
        print("Required changes:")
        for change in result["changes"]:
            role_change = (
                f"{change['from_role']} → {change['to_role']}"
                if change["from_role"] != change["to_role"]
                else change["from_role"]
            )
            sec_change = (
                f"{change['from_section']} → {change['to_section']}"
                if change["from_section"] != change["to_section"]
                else change["from_section"]
            )
            print(f"  • {change['server']}: {sec_change}, {role_change}")
    else:
        print("No changes required - current configuration is optimal!")

    # Verify constraints
    print("\nConstraint verification:")
    print("-" * 50)

    # Check masters and candidates per rack
    print(f"{'Rack':<6} {'Servers':<8} {'Masters':<10} {'Candidates'}")

    for rack in racks:
        servers_in_rack = [sid for sid, cfg in result["assignment"].items() if cfg["rack"] == rack]
        masters = [sid for sid in servers_in_rack if result["assignment"][sid]["role"] == "master"]
        candidates = [sid for sid in servers_in_rack if result["assignment"][sid]["role"] == "candidate"]
        print(f"{rack:<6} {len(servers_in_rack):<8} {len(masters):<10} {len(candidates)}")
        # if masters and candidates:
        #     print("   ⚠️  WARNING: Both master and candidate in same rack!")

    # Summarize each section
    print()
    print(f"{'Section':<10} {'Servers':>7} {'Masters':>7} {'Candidates':>10} {'Replicas':>8}")
    for section in sections:
        servers_in_section = [sid for sid, cfg in result["assignment"].items() if cfg["section"] == section]
        masters = [sid for sid in servers_in_section if result["assignment"][sid]["role"] == "master"]
        candidates = [sid for sid in servers_in_section if result["assignment"][sid]["role"] == "candidate"]
        replicas = [sid for sid in servers_in_section if result["assignment"][sid]["role"] == "replica"]
        print(f"{section:<10} {len(servers_in_section):>7} {len(masters):>7} {len(candidates):>10} {len(replicas):>8}")


if __name__ == "__main__":
    main()
