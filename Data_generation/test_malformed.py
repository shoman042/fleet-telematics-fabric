"""
test_malformed.py - Day-1 test: does the path Eventstream -> Eventhouse keep a MALFORMED message?

Sends two messages to one endpoint:
  1. BAD-TEST-CONTROL-001 : a valid message (control: proves the path works)
  2. BAD-TEST-001         : a valid message cut short (truncated JSON)

Usage
  python test_malformed.py                    sends to the gps endpoint
  python test_malformed.py --signal engine
Run it only AFTER the Eventhouse table of that signal exists (the wizard would otherwise use the broken message as its sample).
"""
import argparse
import json
import sys

from dotenv import load_dotenv

from test_all import SIGNALS, build_message, check_settings, get_settings, make_producer, send_and_wait


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--signal", choices=SIGNALS, default="gps")
    args = ap.parse_args()
    load_dotenv()

    values, missing = get_settings(args.signal)
    if missing:
        sys.exit(f"[FAIL] missing in .env: {', '.join(missing)}")
    bootstrap, topic, conn = values
    for w in check_settings(bootstrap, topic, conn):
        print(f"[!] {w}")
    producer = make_producer(bootstrap, conn)

    control = build_message(args.signal, 1)
    control["event_id"] = "BAD-TEST-CONTROL-001"
    bad = build_message(args.signal, 2)
    bad["event_id"] = "BAD-TEST-001"
    text = json.dumps(bad, separators=(",", ":"))
    payload = text[: int(len(text) * 0.6)]
    assert "BAD-TEST-001" in payload, "the cut removed the event_id"
    try:
        json.loads(payload)
        sys.exit("[FAIL] the truncated text is still valid JSON (script problem)")
    except ValueError:
        pass

    ok = True
    for label, body in (("control (valid)", json.dumps(control, separators=(",", ":"))), ("malformed", payload)):
        res = send_and_wait(producer, topic, "V001", body)
        if "error" in res:
            ok = False
            print(f"[FAIL] {label}: {res['error']}")
        else:
            print(f"[OK]   {label}: DELIVERED partition={res['partition']} offset={res['offset']}")
    print(f"\nSent text of the malformed message:\n{payload}\n")
    table = f"bronze_{args.signal}_raw"
    print("Wait 1 to 5 minutes, then run in the KQL database:")
    print(f"  {table}\n  | where raw_value has 'BAD-TEST'\n  | project raw_value")
    print("Pass = 2 rows (the control and the truncated text). Control only = the path drops malformed messages.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()