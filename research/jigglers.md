# Mouse-jiggler / keep-active behavior at the input-event level

Research for the KeySpark liveness classifier. Goal: ground the non-human (input
automation) synthetic class in how real keep-active products actually behave, so
`keyspark/botgen.py` emits realistic events instead of invented jitter.

Method: deep-research fan-out (6 search angles, 24 sources fetched, 106 claims
extracted, top 25 adversarially verified at 2-of-3 vote). Date: 2026-06.
Convention: plain hyphens, no em dashes.

## The one finding that matters most

Keep-active tools split into two classes, and the split decides whether KeySpark
can see them at all:

- **No-input idle-suppressors** keep the machine awake purely through an OS power
  API and emit ZERO mouse/keyboard events. They are invisible to any input-event
  detector by construction. (Windows: `SetThreadExecutionState` with
  `ES_SYSTEM_REQUIRED` / `ES_DISPLAY_REQUIRED` / `ES_CONTINUOUS`. macOS:
  `IOPMAssertionCreateWithName` / `PreventUserIdleSystemSleep`.)
- **Input-emitting movers** inject genuine OS-level events (Win32 `SendInput`,
  macOS `CGEvent`) or are real USB HID hardware. These ARE visible in the event
  stream and are what KeySpark can flag.

A third, smaller class is **keyboard keep-awake** tools that emit a single
non-character keystroke (F15) on a slow timer - input-emitting, but keyboard not
mouse.

## A. Per-product table

Evidence tags: **[V]** verified from primary source code or OS-observable
behavior, **[M]** vendor marketing only, **[U]** unconfirmed / second-hand.

| Product (category) | Q1 Input vs idle-suppress | Q2 Geometry / step px | Q3 Timing | Q4 Randomization | Q5 Keyboard | Q6 HW vs SW | Evidence |
|---|---|---|---|---|---|---|---|
| **PowerToys Awake** (sw, Win) | No-input idle-suppressor | none | n/a | none | none | Power API (`SetThreadExecutionState`); no input APIs even imported | **[V]** source-level |
| **macOS `caffeinate`** (sw, CLI) | No-input idle-suppressor | none | n/a | none | none | Power API (`IOPMAssertion`) | **[V]** behavior; **[U]** cited by analogy only |
| **Amphetamine** (sw, macOS) | No-input idle-suppressor (has OPTIONAL opt-in mouse-move mode) | optional mode not characterized | optional mode "periodic" | none in default mode | none | Power API (`IOPMAssertionCreateWithName` / `PreventUserIdleSystemSleep`) | **[V]** OS-observable via `pmset -g assertions` |
| **Caffeine for Windows** (sw, Win) | Emits input (keyboard) | n/a (no mouse) | ~ every 59 s, fixed | none reported | Sends **F15** keystroke | Software injection (assumed) | **[U]** second-hand via PowerToys issue #4246 |
| **Mouse Jiggler** (arkane-systems fork; sw, Win) | Emits input (mouse) | Relative nudge, fixed pattern: Normal +/-4px diagonal; Linear +/-4px horizontal; Circle 8-step octagon 2-3px; Zen (0,0) | Fixed `JigglePeriod` s, OR randomized | YES - timing only: uniform re-roll [1 s .. period] each tick (CLI caps period at 60 s) | none (no keyboard path in source) | Software injection: `SendInput` + `INPUT_MOUSE` + `MOUSEEVENTF_MOVE` (relative, never absolute) | **[V]** source-level |
| **Move Mouse** (sw, Win) | Mover (input mechanism unresolved) | not reliably established | Default 30 s; randomizable 30-60 s (constant or random low/high bounds) | YES - randomized interval bounds | configurable actions, not characterized | Software (assumed) | **[V]** timing; **[U]** input-emission mechanism (claim refuted 1-2) |
| **VAYDEER / "Undetectable Mouse Mover"** USB dongle (hw) | Emits input (genuine HID) | Marketed "irregular / human-like" movement; physical-turntable variant rotates a real mouse | Marketed randomized intervals | Marketed "undetectable / randomized" | some composite HID models may send keys (unconfirmed) | **Real USB HID device** (OS sees a genuine mouse) | **[M]** vendor listing; **[U]** no teardown verified |
| **Phone/watch-based jigglers** (hw/physical) | Likely pure physical analog motion (no synthetic events) | n/a - real hand-equivalent motion | n/a | n/a | n/a | Physical motion of a real device, or app-driven on a phone | **[U]** entirely unconfirmed |

### Per-product notes (verified items)

- **PowerToys Awake [V, gold].** `Bridge.cs` declares exactly one power P/Invoke
  (`SetThreadExecutionState`); `Manager.cs` returns `ES_SYSTEM_REQUIRED |
  ES_CONTINUOUS` (plus `ES_DISPLAY_REQUIRED` when keep-display-on). A grep of the
  Awake module finds ZERO references to `SendInput` / `keybd_event` /
  `mouse_event` / `MOUSEEVENTF` - it physically cannot emit input. Two open
  feature requests (#21430 "Jiggle mouse", #19866 "Mouse Jiggler") ask PowerToys
  to ADD jiggling, which only makes sense because Awake does not move the mouse.
  The documented "fails on the lock screen" limitation is consistent with a
  user-mode power assertion, not input injection.
- **Mouse Jiggler, arkane-systems fork [V, gold].** This is the actively
  maintained descendant of the "classic" Windows Mouse Jiggler. `Helpers.cs` has
  one input path: build `INPUT{ type=INPUT_MOUSE, mi={ dx, dy,
  dwFlags=MOUSEEVENTF_MOVE } }` then `SendInput(...)`. `MOUSEEVENTF_ABSOLUTE`
  (0x8000) is absent, so dx/dy are RELATIVE deltas - it nudges, it never jumps to
  an absolute point. No `INPUT_KEYBOARD` anywhere, so it sends no keys.
  `JigglePatterns.cs` (verbatim):
  - `Normal  = { (4d, 4d), (-4d, -4d) }`  diagonal back-and-forth
  - `Linear  = { (4d, 0), (-4d, 0) }`  horizontal back-and-forth
  - `Circle  = { (3d,2d),(2d,3d),(-2d,3d),(-3d,2d),(-3d,-2d),(-2d,-3d),(2d,-3d),(3d,-2d) }`  closed octagon, sums to (0,0)
  - `Zen     = { (0,0) }`  zero-displacement, but still dispatches a `SendInput` event
  where `d` is an integer multiplier (default 1, so 4px). Geometry has no RNG -
  it is fully deterministic; the back-and-forth means the pointer oscillates
  between two fixed points, giving near-constant step size.
  Timing (`MainForm.cs` tick handler): fixed = `JigglePeriod * 1000` ms;
  randomized (`RandomTimer`) = `Random.Shared.Next(1, JigglePeriod+1) * 1000`,
  re-rolled every tick = uniform integer 1 s .. period inclusive.
  Author's README, candidly AGAINST his own product: "Mouse Jiggler is easily
  detectable by any decent monitoring software... anything that doesn't use
  rootkit-style techniques to hide," and steers evasion-seekers to hardware.
- **Amphetamine [V, OS-observable].** Keep-awake is a power assertion, verifiable
  on any Mac with `pmset -g assertions | grep Amphetamine` ->
  `PreventUserIdleSystemSleep`. Its "Periodic Mouse Movement" is an explicit
  opt-in toggle ("system awake but screen off"), NOT the core mechanism. If a
  user enables it, Amphetamine crosses into the mover tier, but the geometry and
  interval of that optional mode were not characterized.
- **Design-history confirmation [V].** PowerToys issue #4246 (which became Awake)
  contains the verbatim rationale: "the caffeine software uses key strokes which
  is less than desirable than an API approach that amphetamine takes." This is
  the canonical evidence for the Caffeine-F15 vs Amphetamine-power-API split and
  why a modern tool deliberately chose to emit nothing.

## B. Behavioral model spec - 3 encodable tiers

All numbers below are grounded in the verified sources unless tagged [M]/[U].
The single most important encoding fact: **across every verified open-source
tool, the only randomization implemented is timing/interval jitter. No verified
tool randomizes the movement geometry** (no Bezier, no human-like curve, no
random zig-zag, no jump-to-random-point). Human-like-path claims live entirely in
the unverified hardware/marketing tier.

### Tier A - no-input idle-suppressors

- Event types: **NONE**. Emits no mouse, no keyboard events.
- Geometry / step px / interval / randomization: not applicable.
- Mechanism: OS power assertion (`SetThreadExecutionState` ES_* on Windows;
  `IOPMAssertionCreateWithName` / `PreventUserIdleSystemSleep` on macOS).
- Members: PowerToys Awake, `caffeinate`, Amphetamine (default), Caffeine's
  API-style peers.
- Encoding: there is nothing to emit. This tier cannot be represented as input
  events and should not be faked as such. See the scope note (C).

### Tier B - crude periodic movers

- Event type: relative mouse `move` (`MOUSEEVENTF_MOVE`, never absolute).
- Geometry: fixed deterministic pattern -
  - diagonal back-and-forth between two points (alternating +s, -s per axis), or
  - horizontal-only back-and-forth, or
  - small closed octagon (circle approximation), or
  - degenerate zero-displacement move (Zen: an event at the same coordinates).
- Step size: ~4 px per axis at default (about 5.7 px diagonal displacement);
  2-3 px per step for the octagon. Integer-scalable up if the user raises the
  distance multiplier. These are TINY moves.
- Interval: FIXED periodic. Commonly 30 s (Move Mouse default) down to
  single-digit seconds (configurable). Implies a very SPARSE event stream:
  roughly 1-12 events in a 60 s window, often only 1-2.
- Randomization: none. Interval CV is near zero; step size CV is near zero
  (oscillation between two fixed points). This regularity is the signal.
- Members: Mouse Jiggler (Normal/Linear/Circle/Zen, fixed-timer mode); Move
  Mouse (default 30 s).

### Tier C - randomized / evasive movers

- Event type and geometry: SAME as Tier B (relative move, same fixed patterns).
  The geometry does NOT become human-like; only the clock changes.
- Step size: same as Tier B (~2-6 px).
- Interval: RANDOMIZED. Verified scheme = uniform re-roll each tick in
  [1 s .. period], period commonly capped around 60 s (Mouse Jiggler
  `RandomTimer`). Move Mouse offers random low/high bounds (e.g. 30-60 s).
- Randomization scheme: **timing jitter only**. The interval distribution is
  uniform (a flat random spread), which is distinct from human burstiness
  (humans cluster then pause). So even "evasive" software has a tell: uniform
  inter-event gaps plus rigid, repeating micro-geometry.
- [M]/[U] hardware extension: "undetectable" USB dongles market true randomized
  intervals and human-like paths, and being genuine HID they look like a real
  mouse to the OS. This is the realistic hard case but is unverified - treat any
  curved-path / Bezier modeling as a marketing hypothesis, not observed fact.

### Optional Tier B'/C' - keyboard keep-awake (encodable variant)

- Event type: `key_down` of a single non-character key, classically **F15**
  (also attributed in the wild: F13-F24, Scroll Lock toggle on/off, Shift).
- Key diversity: effectively 1 distinct key over N strokes -> very low.
- Interval: slow and fixed, ~ every 59 s for Caffeine [U].
- Mechanism: software keystroke injection.
- Note: distinct from a normal auto-typer. It is one held/repeated function key
  on a slow cadence, not fast letter typing.

## C. Scope note - what an input-event detector can and cannot see

- **Tier A is invisible by construction.** No events reach the stream, so the
  KeySpark featurizer produces no window rows for these users. They are NOT a
  failure of the classifier; they are out of band. Detecting them requires
  out-of-stream signals (enumerate active power assertions via `pmset -g
  assertions` on macOS or execution-state requests on Windows, or process
  inspection) - data KeySpark does not currently ingest. State this explicitly in
  the paper: an event-stream liveness model flags movers, not idle-suppressors.
- **Tiers B and C are detectable.** They inject genuine OS-level events and, per
  the Mouse Jiggler author, are "easily detectable by any decent monitoring
  software" absent rootkit hiding. Their tells are exactly KeySpark's features:
  low `iei_cv` (fixed timer) or a flat-uniform `iei_cv` (randomized timer) rather
  than human burstiness, very low `step_cv` from repeating micro-geometry, small
  `step_mean`, and `mouse_fraction` near 1.0 (or, for keyboard keep-awake, near
  0.0 with `key_diversity` near 0).
- **The genuine-HID hardware tier is the open boundary.** A USB dongle produces a
  real HID event stream (not software-injected), so on macOS it survives capture
  paths that drop synthetic `CGEvent`s, and a good dongle can randomize timing.
  Geometry regularity and tiny step size are still likely tells, but this tier
  was not verified and is the main residual risk.

## Encoding implications for botgen (for the next prompt, not changed here)

These follow directly from the verified behavior and are where the current
generator diverges from reality. Recorded as a spec, no code touched.

1. **Real movers are SPARSE and SLOW.** Verified intervals are seconds to 30-60 s,
   not the current `_BASE_RANGE` jiggler 0.3-2.0 s. A realistic periodic mover
   yields ~1-12 move events per minute, sometimes too few to even form features.
   Model a slow cadence band (roughly 1-60 s) for the mover class.
2. **Steps are TINY and near-constant.** Verified ~2-6 px, not the current
   12 px / 60 px. And back-and-forth oscillation gives LOW `step_cv`; the current
   `randint(-reach, reach)` random walk actually produces MORE step variation
   than a real jiggler, which understates the robotic regularity the model should
   learn. Consider an alternating two-point nudge or fixed octagon.
3. **Movement is relative, never jump-to-random-point.** The current relative
   walk matches direction; just shrink and regularize it.
4. **Two timing regimes, both encodable:** fixed period (very low `iei_cv`) and
   uniform-random period in [1 s .. ~60 s] (flat-spread `iei_cv`, distinct from
   human burst-and-pause). These are the crude vs evasive tiers.
5. **Geometry is never randomized in verified software.** Do not model Bezier /
   human-curve paths as the bot class; if included, label them clearly as the
   unverified hardware-marketing hypothesis.
6. **Add a keyboard keep-awake profile:** a single function key (F15, or Scroll
   Lock / F13-F24) repeated on a slow ~59 s fixed cadence -> `key_diversity`
   near 0, low volume. This is more faithful than the current fast a-s-d-f typer.
7. **Tier A has no event representation.** Do not synthesize it as events; treat
   it as an acknowledged scope limit of an input-stream detector.

## Source quality and confidence

- **Gold (primary source code):** PowerToys Awake (`Manager.cs`, `Bridge.cs`,
  Learn docs, issue #4246) and arkane-systems Mouse Jiggler (`Helpers.cs`,
  `JigglePatterns.cs`, `MainForm.cs`). Highest confidence.
- **Strong (OS-observable):** Amphetamine power assertion via `pmset`.
- **Weak / second-hand:** Caffeine-for-Windows F15 at ~59 s comes only from the
  characterization inside PowerToys issue #4246, not Caffeine's own source or a
  teardown. The exact key and interval are plausible but not primary-verified.
- **By analogy only:** `caffeinate`'s no-input behavior was established through
  the Awake maintainer's framing, not an Apple man page.
- **Refuted:** the claim that Move Mouse emits actual input events was killed
  (1-2 vote). Only its timing (default 30 s, randomizable) is reliable; its
  input mechanism and geometry are unresolved.
- **Marketing only:** VAYDEER and other "undetectable mouse mover" dongle
  listings. Treated as claims, not observations.

## What I could not confirm (gaps)

- **Hardware USB-HID dongles** (VAYDEER "Undetectable Mouse Mover", Liberty,
  generic Amazon HID): zero verified claims survived. Genuine-HID step sizes,
  intervals, whether any present as composite keyboard+mouse HID and send keys,
  and whether the "randomized/human-like path" marketing is real - all
  unconfirmed.
- **Phone/watch-based jigglers:** unconfirmed. Most likely pure physical analog
  motion (a phone resting on a trackpad, or watch motion), which would be
  indistinguishable from a real hand and emit no synthetic events at all.
- **Caffeine (Windows):** F15 key and ~59 s interval not primary-verified; modern
  build may also use a power API.
- **Move Mouse:** input-emission mechanism (SendInput vs power API vs both) and
  movement geometry unresolved.
- **Amphetamine optional mouse-move mode:** geometry and interval not
  characterized.

## Sources

Primary (source code / docs / OS-observable):
- PowerToys Awake: https://github.com/microsoft/PowerToys (src/modules/awake -
  Core/Manager.cs, Native/Bridge.cs);
  https://learn.microsoft.com/en-us/windows/powertoys/awake;
  https://github.com/microsoft/PowerToys/blob/main/doc/planning/awake.md;
  https://github.com/microsoft/PowerToys/issues/4246 (Caffeine-vs-Amphetamine
  rationale); feature requests #21430, #19866.
- Mouse Jiggler (arkane-systems): https://github.com/arkane-systems/mousejiggler
  (Helpers.cs, JigglePatterns.cs, MainForm.cs via raw.githubusercontent.com
  master).
- Amphetamine: https://apps.apple.com/us/app/amphetamine/id937984704 ;
  https://kb.offsec.nl/tools/apple-macos/amphetamine (pmset assertions).
- Move Mouse: https://github.com/sw3103/movemouse and its wiki (Scenarios,
  Behaviour).
- macOS injection nuance: https://objective-see.org/blog/blog_0x36.html ;
  https://developer.apple.com/documentation/coregraphics/cgeventsourcestateid .

Marketing / secondary (treat as claims):
- https://www.vaydeer.com/products/vaydeer-undetectable-mouse-mover-with-enlarged-turntable
- https://www.tomshardware.com/how-to/best-mouse-jiggler-methods
- Roundup blogs (umatechnology, helpdeskgeek, ofzenandcomputing, empmonitor).
