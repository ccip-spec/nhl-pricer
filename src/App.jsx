import { useState, useEffect, useCallback, useMemo, useRef, Fragment, Component } from "react";

// ─── SUPABASE CONFIG ─────────────────────────────────────────────────────────
// v52: Cloud sync is now configurable at runtime via Settings → Cloud Sync card.
// Users enter their own Supabase URL + anon key (saved to localStorage).
// This enables Push/Pull across devices without requiring code edits.
// NOTE: Explicit Push/Pull model was chosen over auto-sync to avoid silent
// data-loss scenarios (race conditions, offline sync failures, merge conflicts).
// TODO (future): Add real-time auto-sync with conflict resolution once the
//                Push/Pull flow is validated in practice.
function getSbConfig() {
  try {
    const raw = localStorage.getItem("nhl_sb_config");
    if (!raw) return { url: "", key: "", device: "" };
    return JSON.parse(raw);
  } catch { return { url: "", key: "", device: "" }; }
}
function setSbConfig(cfg) {
  try { localStorage.setItem("nhl_sb_config", JSON.stringify(cfg)); } catch {}
}
function isSbEnabled() {
  const c = getSbConfig();
  return !!(c.url && c.key);
}
async function sbLoad(key) {
  const cfg = getSbConfig();
  if (!cfg.url || !cfg.key) return null;
  try {
    const r = await fetch(`${cfg.url}/rest/v1/pricer_state?key=eq.${encodeURIComponent(key)}&select=value,updated_at,device`, {
      headers: { apikey: cfg.key, Authorization: `Bearer ${cfg.key}` }
    });
    const d = await r.json();
    return d?.[0] ?? null;   // returns {value, updated_at, device} or null
  } catch { return null; }
}
async function sbSave(key, value, device) {
  const cfg = getSbConfig();
  if (!cfg.url || !cfg.key) return false;
  try {
    const r = await fetch(`${cfg.url}/rest/v1/pricer_state`, {
      method: "POST",
      headers: { apikey: cfg.key, Authorization: `Bearer ${cfg.key}`,
        "Content-Type": "application/json", Prefer: "resolution=merge-duplicates" },
      body: JSON.stringify({ key, value, device: device || "unknown", updated_at: new Date().toISOString() })
    });
    return r.ok;
  } catch { return false; }
}

// ─── TEAM METADATA ────────────────────────────────────────────────────────────
const TEAM_NAMES = {
  ANA:"Ducks",BOS:"Bruins",BUF:"Sabres",CAR:"Hurricanes",COL:"Avalanche",
  DAL:"Stars",EDM:"Oilers",LAK:"Kings",MIN:"Wild",MTL:"Canadiens",
  OTT:"Senators",PHI:"Flyers",PIT:"Penguins",TBL:"Lightning",UTA:"Mammoth",
  VEG:"Golden Knights"
};
const PLAYOFF_TEAMS = Object.keys(TEAM_NAMES);
const HOME_PATTERN = [null,1,1,0,0,1,0,1];

// v43: name normalization for cross-source matching (HR vs MoneyPuck vs box-score parser).
// Strips diacritics, lowercases, removes punctuation/whitespace, and folds common first-name
// nickname variants so "Josh Norris" and "Joshua Norris" hash to the same key.
const NICKNAME_MAP = {
  // Player canonical-form aliases. LEFT side is what we'll FOLD INTO right side.
  // Scoring/popular aliases
  "joshua": "josh",
  "alexander": "alex",
  "alexandre": "alex",
  "alexandr": "alex",
  "aleksander": "alex",
  "aleksandr": "alex",
  "mathew": "matt",
  "matthew": "matt",
  "mathieu": "matt",
  "matthias": "matt",
  "michael": "mike",
  "nicholas": "nick",
  "nicolas": "nick",
  "nikolai": "nik",
  "anthony": "tony",
  "william": "will",
  "robert": "rob",
  "richard": "rick",
  "thomas": "tom",
  "andrew": "andy",
  "patrick": "pat",
  "joseph": "joe",
  "samuel": "sam",
  "benjamin": "ben",
  "daniel": "dan",
  "david": "dave",
  "jonathan": "jon",
  "christopher": "chris",
  "kristoffer": "chris",
  // v47: short↔long variants for NHL rosters
  "cameron": "cam",
  "zachary": "zach",
  "zack": "zach",
  "nathaniel": "nate",
  "nathan": "nate",
  "timothy": "tim",
  "gregory": "greg",
  // European/Scandinavian
  "oskar": "oscar",
  "eric": "erik",
  "fredrik": "frederik",
  "frederick": "frederik",
  "niklas": "nicklas",
  "mikko": "mike",
  "mikael": "mike",
  "mikkel": "mike",
  "johan": "johannes",
  "henri": "henry",
  "henrik": "henry",
  "vladimir": "vlad",
  "aleksei": "alex",
  "alexei": "alex",
  "aliaksei": "alex",
  "andreas": "andrew",
  "andrei": "andrew",
  "dmitry": "dmitri",
  "sergei": "sergey",
  // v49: fold Max/Maxwell/Maxim family to "max" (not "maxime" — NHL context prefers "Max")
  "maxwell": "max",
  "maxim": "max",
  "maksim": "max",
  "maxime": "max",
  // v49: more diminutives
  "tommy": "tom",
  "thomas": "tom",
  "mikey": "mike",
  "jacob": "jake",
  "jakob": "jake",
  "rob": "robert",
  "robby": "robert",
  "bobby": "robert",
  "willy": "will",
  "billy": "will",
  "charlie": "charles",
  "chuck": "charles",
  "danny": "dan",
  "donny": "don",
  "donnie": "don",
  "donald": "don",
  "jimmy": "jim",
  "james": "jim",
  "johnny": "john",
  "jonny": "jon",
  "jackson": "jack",
  "jakson": "jack",
  "zeke": "ezekiel",
  "sammy": "sam",
  "harry": "harold",
  "larry": "lawrence",
  "artyom": "artemi",
  "artemiy": "artemi",
  // v50: Russian transliteration variants
  "daniil": "danil",
  "danila": "danil",
};

// v49: explicit last-name alias map for known typos / variants that can't be handled by general rules.
// LEFT side is what we FOLD INTO right side. Keys are the normalized (lowercased, diacritic-stripped) form.
const LAST_NAME_MAP = {
  "sttzle": "stutzle",   // Stützle typo
  "bck": "back",         // Bäck → Back typo (missing 'a')
};
function normPlayerName(s) {
  let n = (s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/[^a-z0-9 ]/g,"").trim();
  // v50: collapse multi-word names to first + last, dropping middle names/hyphens.
  //      Handles "Emil Martinsen Lilleber" ↔ "Emil Lilleber", "J.T. Compher" ↔ "JT Compher".
  //      Fold first-name nicknames and last-name suffix/alias rules.
  let parts = n.split(/\s+/);
  if (parts.length >= 3) {
    // Keep only first + last
    parts = [parts[0], parts[parts.length - 1]];
  }
  if (parts.length > 0 && NICKNAME_MAP[parts[0]]) parts[0] = NICKNAME_MAP[parts[0]];
  // v49: last-name suffix normalization — handles Slafkovsky/Slafkovsk, Malkin/Malkine, etc.
  if (parts.length > 1) {
    const lastIdx = parts.length - 1;
    let last = parts[lastIdx];
    // Trailing -sky / -ski / -skyi -> -sk
    last = last.replace(/sk[yie]+$/, "sk");
    // Explicit aliases (typos, known variants)
    if (LAST_NAME_MAP[last]) last = LAST_NAME_MAP[last];
    parts[lastIdx] = last;
  }
  return parts.join("").replace(/\s+/g,"");
}
// Deduplicate a player array by normalized name+team. Merges stat fields by summing,
// keeps the longer name (more complete spelling), prefers HR team over MoneyPuck team.
// v50: SCRATCHED role sticks — if ANY duplicate is SCRATCHED, the merged player is SCRATCHED.
//      Prefer non-empty role over empty, otherwise longer-name record wins.
function dedupePlayers(players) {
  if (!Array.isArray(players)) return players;
  const byKey = new Map();
  const STAT_FIELDS = ["pGP","pG","pA","pPts","pSOG","pHIT","pBLK","pTK","pGV","pPIM","pTOI"];
  for (const p of players) {
    const key = normPlayerName(p.name) + "|" + (p.team||"");
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, {...p});
    } else {
      // Merge: keep longer name spelling
      if ((p.name||"").length > (existing.name||"").length) existing.name = p.name;
      // Sum playoff stats
      for (const f of STAT_FIELDS) {
        if (p[f] != null) existing[f] = (existing[f] || 0) + (p[f] || 0);
      }
      // v50: SCRATCHED wins — user explicitly marked this player out
      // v56: also respect new IR/INACTIVE roles
      const OUT = new Set(["SCRATCHED","INACTIVE","IR"]);
      if (OUT.has(p.lineRole) || OUT.has(existing.lineRole)) {
        existing.lineRole = OUT.has(p.lineRole) ? p.lineRole : existing.lineRole;
      } else if (!existing.lineRole && p.lineRole) {
        existing.lineRole = p.lineRole;
      }
      if (!existing.team && p.team) existing.team = p.team;
    }
  }
  return Array.from(byKey.values());
}

// ─── MATH ─────────────────────────────────────────────────────────────────────
function poissonPMF(k, lam) {
  if (lam <= 0) return k === 0 ? 1 : 0;
  let log = -lam + k * Math.log(lam);
  for (let i = 1; i <= k; i++) log -= Math.log(i);
  return Math.exp(log);
}
function poissonCDF(k, lam) {
  let s = 0; for (let i = 0; i <= k; i++) s += poissonPMF(i, lam); return Math.min(1, s);
}
function nbPMF(k, mu, r) {
  // v46: r → ∞ is Poisson limit. Guard against bad inputs and use Poisson when dispersion is large.
  if (mu <= 0) return k === 0 ? 1 : 0;
  if (!isFinite(r) || r >= 100) return poissonPMF(k, mu);
  if (r <= 0) return poissonPMF(k, mu); // safety
  const p = r / (r + mu); let lp = 0;
  for (let i = 0; i < k; i++) lp += Math.log(r + i) - Math.log(i + 1);
  lp += r * Math.log(p) + k * Math.log(1 - p); return Math.exp(lp);
}
function nbCDF(k, mu, r) {
  let s = 0; for (let i = 0; i <= k; i++) s += nbPMF(i, mu, r); return Math.min(1, s);
}
function computeLeaderProbs(lambdas, kMax = 20) {
  const n = lambdas.length, probs = new Array(n).fill(0);
  for (let k = 0; k <= kMax; k++) {
    const cdfs = lambdas.map(l => poissonCDF(k, l));
    const pmfs = lambdas.map(l => poissonPMF(k, l));
    let prod = 1; for (let j = 0; j < n; j++) prod *= cdfs[j];
    for (let i = 0; i < n; i++) { if (cdfs[i] > 1e-15) probs[i] += prod / cdfs[i] * pmfs[i]; }
  }
  return probs;
}

// v24: Monte Carlo leader market.
// Samples each player's future production from NB(futureLam, r), adds realized `actual`,
// finds the max across the pool, splits leader credit fractionally across ties.
// Handles ties correctly (unlike the closed-form computeLeaderProbs which assumes no ties).
// Independence assumption across players is retained for v1; correlation layer comes later.

// Seeded PRNG (mulberry32) for reproducible sims across renders.
function mulberry32(seed) {
  let a = (seed|0) || 0x9e3779b9;
  return function() {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
// Gamma sample (Marsaglia-Tsang) for shape >= 1; falls back to scaled-gamma for shape<1.
// Used as the latent in NB = Poisson(Gamma).
function sampleGamma(shape, rng) {
  if (shape < 1) {
    // Ahrens-Dieter via boost: sample gamma(shape+1) * U^(1/shape)
    const g = sampleGamma(shape + 1, rng);
    const u = rng();
    return g * Math.pow(u, 1 / shape);
  }
  const d = shape - 1/3, c = 1 / Math.sqrt(9 * d);
  while (true) {
    let x, v;
    do {
      // Box-Muller for normal
      const u1 = rng(), u2 = rng();
      const z = Math.sqrt(-2 * Math.log(Math.max(u1, 1e-300))) * Math.cos(2 * Math.PI * u2);
      x = z;
      v = 1 + c * x;
    } while (v <= 0);
    v = v * v * v;
    const u = rng();
    if (u < 1 - 0.0331 * x * x * x * x) return d * v;
    if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v;
  }
}
// Poisson sampler (Knuth for small lam, rejection for large)
function samplePoisson(lam, rng) {
  if (lam < 30) {
    const L = Math.exp(-lam);
    let k = 0, p = 1;
    do { k++; p *= rng(); } while (p > L);
    return k - 1;
  } else {
    // Normal approximation then round, then bump
    const u1 = rng(), u2 = rng();
    const z = Math.sqrt(-2 * Math.log(Math.max(u1, 1e-300))) * Math.cos(2 * Math.PI * u2);
    return Math.max(0, Math.round(lam + Math.sqrt(lam) * z));
  }
}
// NB sample: gamma-poisson mixture. NB(mu, r) with variance = mu + mu^2/r.
// Fast path: when r is very large, NB degenerates to Poisson.
function sampleNB(mu, r, rng) {
  if (mu <= 0) return 0;
  if (r >= 50) return samplePoisson(mu, rng);
  const lam = (sampleGamma(r, rng) / r) * mu; // Gamma(shape=r, scale=mu/r) => mean mu
  return samplePoisson(lam, rng);
}

// Core Monte Carlo leader sim.
// Inputs: array of {futureLam, actual} per player; r = NB dispersion; trials.
// Returns: array of winProb per player (sums to 1, split fractionally on ties).
function simulateLeader(entries, r, trials = 20000, seed = 12345) {
  const n = entries.length;
  const rng = mulberry32(seed);
  const wins = new Array(n).fill(0);
  const sample = new Array(n);
  for (let t = 0; t < trials; t++) {
    let maxVal = -1;
    for (let i = 0; i < n; i++) {
      const draw = sampleNB(entries[i].futureLam, r, rng) + entries[i].actual;
      sample[i] = draw;
      if (draw > maxVal) maxVal = draw;
    }
    // Count tied leaders, split credit 1/K
    let tiedCount = 0;
    for (let i = 0; i < n; i++) if (sample[i] === maxVal) tiedCount++;
    const credit = 1 / tiedCount;
    for (let i = 0; i < n; i++) if (sample[i] === maxVal) wins[i] += credit;
  }
  return wins.map(w => w / trials);
}

// v24 Phase E Pt3: cross-series leader sim using per-player PMFs.
// Use this when each player's distribution is already a full PMF (e.g., from simulateSeries),
// which means teammate correlation is baked in. Players across different matchups are independent.
// Pre-builds cumulative distributions for fast sampling.
function samplePMFFromCDF(cdf, rng) {
  const u = rng();
  // Binary search over cdf
  let lo = 0, hi = cdf.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cdf[mid] < u) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
function simulateLeaderFromPMFs(pmfs, trials = 20000, seed = 99991) {
  const n = pmfs.length;
  if (!n) return [];
  const rng = mulberry32(seed);
  // Build CDFs once
  const cdfs = pmfs.map(pmf => {
    const c = new Array(pmf.length);
    let s = 0;
    for (let i = 0; i < pmf.length; i++) { s += pmf[i]; c[i] = s; }
    // Guard against probability sum < 1 (numerical); clamp last value to 1
    if (c.length) c[c.length-1] = 1;
    return c;
  });
  const wins = new Array(n).fill(0);
  const sample = new Array(n);
  for (let t = 0; t < trials; t++) {
    let maxVal = -1;
    for (let i = 0; i < n; i++) {
      const draw = samplePMFFromCDF(cdfs[i], rng);
      sample[i] = draw;
      if (draw > maxVal) maxVal = draw;
    }
    let tiedCount = 0;
    for (let i = 0; i < n; i++) if (sample[i] === maxVal) tiedCount++;
    const credit = 1 / tiedCount;
    for (let i = 0; i < n; i++) if (sample[i] === maxVal) wins[i] += credit;
  }
  return wins.map(w => w / trials);
}

// v24 Phase E: Unified series simulation with L1 (score-correlated) player production.
// Returns a full SimResult with all aggregates needed for every market.
// Single source of truth — every market panel reads from this.

// Sample categorical from weights (sums not required to equal 1, uses cumulative)
function sampleCategorical(weights, total, rng) {
  if (total <= 0) return 0;
  const u = rng() * total;
  let cum = 0;
  for (let i = 0; i < weights.length; i++) {
    cum += weights[i];
    if (u < cum) return i;
  }
  return weights.length - 1;
}

// Multinomial: distribute k trials across weights. Slow path would be k×cat-sample; we use that since k is small (typical game = 2-8 goals).
function multinomialDraws(k, weights, total, rng) {
  const out = new Array(weights.length).fill(0);
  for (let i = 0; i < k; i++) {
    const idx = sampleCategorical(weights, total, rng);
    out[idx]++;
  }
  return out;
}

// Build simulation inputs once per (series, player, stat) call: future-lambda per player per stat per game.
// Returns a SimInputs object capturing everything the sim needs.
// NOTE: the caller must precompute effG (with winPct, expTotal, pOT, result applied) and the goalie multipliers per game.
function buildSimInputs(effG, homeAbbr, awayAbbr, players, globals, goalieQualityFaced, pGamePlayed, linemates, currentRound) {
  const BASELINE = 5.82;
  const STATS = ["g","a","sog","hit","blk","tk","pim","give"];
  // Filter to active skaters on either team
  const pool = (players||[]).filter(p => (p.team===homeAbbr || p.team===awayAbbr) && roleMultiplier(p.lineRole) > 0);
  // For each player, precompute per-stat per-game future lambda.
  // (If user has already entered per-game stats in pGames, those contribute to realized `actual`; the sim only models FUTURE games.)
  const pgKey = (stat) => stat==="tk"?"take_pg":stat==="give"?"give_pg":stat==="pim"?"pim_pg":stat+"_pg";
  // v98: actual seed must be ROUND-FILTERED. Previously used p.pG/p.pA/etc which are cumulative across all rounds.
  // For an R2 series sim, that meant a player's R1 goals were seeded as already-scored R2 series goals,
  // wildly inflating their leader probability. e.g. Boldy (6G in R1) showed 95% to lead R2 because
  // the sim treated him as starting R2 already up 6-0 on everyone else.
  // Now: use readActual(p, stat, currentRound) which filters pGames by round.
  const playerData = pool.map(p => {
    const isHome = p.team === homeAbbr;
    const perStat = {};
    for (const stat of STATS) {
      // v53: role multiplier is stat-aware now (TOP6 +bump for scoring, -penalty for hits)
      const rm = roleMultiplier(p.lineRole, stat);
      const shrunk = shrinkRate(p[pgKey(stat)], p.gp, stat);
      // v68: NO blend here. Sim drives N+ / O/U / 1+ markets — blend was over-weighting
      // playoff sample for those markets. Series Leader has its own blend at site 2.
      const rr = shrunk * rm * globals.rateDiscount * statRateMultiplier(stat);
      // v98: round-aware realized seed (was cumulative across all rounds)
      const actual = readActual(p, stat, currentRound) || 0;
      // Per-game future lambda for game i (if game played already -> 0; else rr × scale × goalieMult)
      const perGameFuture = [];
      for (let i = 0; i < 7; i++) {
        const g = effG[i];
        if (g.result) { perGameFuture.push(0); continue; }
        const pPlay = pGamePlayed[i+1] ?? 0;
        if (pPlay <= 0) { perGameFuture.push(0); continue; }
        const scale = SCORING_STATS.has(stat) ? ((g.expTotal||BASELINE)/BASELINE) : 1;
        const goalieMult = SCORING_STATS.has(stat)
          ? (isHome ? goalieQualityFaced[i].faceByHome : goalieQualityFaced[i].faceByAway)
          : 1.0;
        // expected pPlay-scaled lambda (pPlay < 1 means the game may not happen; we'll roll pPlay at sim time)
        perGameFuture.push(rr * scale * goalieMult);
      }
      perStat[stat] = { actual, perGameFuture };
    }
    return { name: p.name, team: p.team, isHome, perStat };
  });

  // v57: linemate lookup — for each pool player, indices of their top-3 linemates (same-team only).
  // Match MoneyPuck (last name only, lowercase) → our player records (full name).
  // If linemates map isn't provided, this is an empty lookup (falls back to uniform assist weights).
  const lastNameNorm = (fullName) => {
    const cleaned = (fullName||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/[^a-z\s]/g,"").trim();
    const parts = cleaned.split(/\s+/);
    return parts.length ? parts[parts.length - 1] : "";
  };
  // Build pool last-name index for fast same-team mate lookup
  const poolIdxByLastTeam = new Map();  // "lastname|team" → poolIdx
  for (let i = 0; i < playerData.length; i++) {
    const p = players.find(x => x.name === playerData[i].name && x.team === playerData[i].team);
    if (!p) continue;
    poolIdxByLastTeam.set(lastNameNorm(p.name) + "|" + playerData[i].team, i);
  }
  const linemateIdx = playerData.map((pp,i) => {
    if (!linemates) return [];
    const p = players.find(x => x.name === pp.name && x.team === pp.team);
    if (!p) return [];
    const key = lastNameNorm(p.name) + "|" + pp.team;
    const mates = linemates[key];
    if (!mates || !mates.length) return [];
    return mates
      .map(m => poolIdxByLastTeam.get(m.mate + "|" + pp.team))
      .filter(idx => idx != null && idx !== i);
  });

  return { pool: playerData, effG, homeAbbr, awayAbbr, pGamePlayed, linemateIdx };
}

// Core series simulator.
// For each sim: decide game-by-game using g.winPct (determines series length + winner).
// For scoring-stats (g, a): draw team goals ~ Poisson(expTotal × side_winPct), distribute via multinomial
//   across team's skater pool weighted by each player's per-game goal lambda. Assists: ~1.7 per goal,
//   distributed among teammates similarly weighted by assist lambda.
// For non-scoring stats (sog, hit, blk, tk, pim, give): each player draws independently from NB(futureLam_game, r).
// Returns {trials, serieLengthCounts, winnerCounts[H,A], exactScoreCounts{"4-0"..}, gameGoalsHist, teamGoalsHist{home,away},
//   shutoutCount, otCount, playerTotalsHist: {name,team: {stat: [counts per threshold]}}}
function simulateSeries(inputs, r, trials = 20000, seed = 31337) {
  const rng = mulberry32(seed);
  const { pool, effG, linemateIdx } = inputs;
  const LINEMATE_BOOST = 3.0;  // v57: boost assist weight for goalscorer's linemates
  const HOME_PATTERN_LOCAL = [false, true, true, false, false, true, false, true]; // 1-indexed like the app
  // Games 1,2 home; 3,4 away; 5 home; 6 away; 7 home (2-2-1-1-1)
  const N = pool.length;

  // Indexes of home vs away skaters in the pool
  const homeIdx = pool.map((p,i)=>p.isHome?i:-1).filter(i=>i>=0);
  const awayIdx = pool.map((p,i)=>!p.isHome?i:-1).filter(i=>i>=0);

  // Output accumulators
  const seriesLenCounts = [0,0,0,0,0,0,0,0]; // index = games played (4..7)
  const exactScoreCounts = {}; // "H-A" e.g. "4-1" means home 4 wins, away 1 win
  const winnerCounts = {H:0, A:0};
  let totalShutouts = 0;
  let totalOT = 0;
  // Game-by-game goal distribution across all games that happen in any sim
  // We'll produce total goals PMF for the *series* (sum over games) and for total shutouts in series.
  const seriesGoalsHist = new Array(120).fill(0); // 0..119 series total goals
  const homeGoalsHist = new Array(60).fill(0);
  const awayGoalsHist = new Array(60).fill(0);
  const seriesShutoutsHist = new Array(8).fill(0);
  const seriesOTHist = new Array(8).fill(0);
  // Per-player series totals across trials. We keep a running sum and histogram by threshold for leader/OU queries.
  // Histogram: for each player × stat, count how many sims produced exactly k. kMax = 60 (covers hit/blk edge cases).
  const KMAX = 60;
  const playerHist = pool.map(()=>({
    g: new Array(KMAX+1).fill(0),
    a: new Array(KMAX+1).fill(0),
    sog: new Array(KMAX+1).fill(0),
    hit: new Array(KMAX+1).fill(0),
    blk: new Array(KMAX+1).fill(0),
    tk: new Array(KMAX+1).fill(0),
    pim: new Array(KMAX+1).fill(0),
    give: new Array(KMAX+1).fill(0),
  }));
  // Leader-market support: per-sim max across pool, tie-split credit
  const leaderWins = {
    g: new Array(N).fill(0), a: new Array(N).fill(0), pts: new Array(N).fill(0),
    sog: new Array(N).fill(0), hit: new Array(N).fill(0), blk: new Array(N).fill(0),
    tk: new Array(N).fill(0), pim: new Array(N).fill(0), give: new Array(N).fill(0),
  };

  // Scratch buffers reused per sim
  const trialTotals = pool.map(()=>({g:0,a:0,sog:0,hit:0,blk:0,tk:0,pim:0,give:0}));
  // Seed totals with realized `actual`
  const actuals = pool.map(p => ({
    g: p.perStat.g.actual, a: p.perStat.a.actual, sog: p.perStat.sog.actual,
    hit: p.perStat.hit.actual, blk: p.perStat.blk.actual, tk: p.perStat.tk.actual,
    pim: p.perStat.pim.actual, give: p.perStat.give.actual,
  }));

  // Realized series state from effG (games already played)
  let realizedHW = 0, realizedAW = 0;
  let realizedGoalsH = 0, realizedGoalsA = 0;
  let realizedShutouts = 0, realizedOT = 0;
  for (const g of effG) {
    if (!g.result) continue;
    if (g.result === "home") realizedHW++; else if (g.result === "away") realizedAW++;
    if (typeof g.homeScore === "number") realizedGoalsH += g.homeScore;
    if (typeof g.awayScore === "number") realizedGoalsA += g.awayScore;
    if (g.homeScore === 0 || g.awayScore === 0) realizedShutouts++;
    if (g.wentOT || g.result === "ot") realizedOT++;
  }

  for (let t = 0; t < trials; t++) {
    // Reset trial totals from actuals
    for (let i = 0; i < N; i++) {
      const a = actuals[i], tt = trialTotals[i];
      tt.g = a.g; tt.a = a.a; tt.sog = a.sog; tt.hit = a.hit;
      tt.blk = a.blk; tt.tk = a.tk; tt.pim = a.pim; tt.give = a.give;
    }
    let hw = realizedHW, aw = realizedAW;
    let trialGoalsH = realizedGoalsH, trialGoalsA = realizedGoalsA;
    let trialShutouts = realizedShutouts, trialOT = realizedOT;

    // Play through remaining games until series ends (4 wins)
    for (let gi = 0; gi < 7; gi++) {
      if (hw >= 4 || aw >= 4) break;
      const g = effG[gi];
      if (g.result) continue; // already played; realized state seeded above
      // Roll game outcome using winPct (perspective: series home team)
      const homeWins = rng() < g.winPct;
      if (homeWins) hw++; else aw++;
      // Draw team goal counts
      // v56 fix: goal share is NOT equal to winPct. A 65% favorite scores ~55% of goals, not 65%.
      //           Empirically, goal share ≈ 0.5 + (winPct - 0.5) × 0.60.
      //           This matters for every downstream: player goals, total goals, shutouts, most-goals.
      const total = g.expTotal || 5.82;
      const goalShare = 0.5 + (g.winPct - 0.5) * 0.60;
      // v92: opposing-goalie quality scaling
      const fH = g.faceByHome ?? 1.0;
      const fA = g.faceByAway ?? 1.0;
      const lamH = Math.max(0.01, total * goalShare * fH);
      const lamA = Math.max(0.01, total * (1 - goalShare) * fA);
      let goalsH = samplePoisson(lamH, rng);
      let goalsA = samplePoisson(lamA, rng);
      // v100: previously, when Poisson sampling produced the "wrong" winner, we force-bumped the
      // intended winner to loser+1 and tagged the game as OT. That made ~40% of all games count
      // as OT (since the Poisson sampling disagrees with the pre-determined winner that often).
      // Result: avgOT was 3.4 in a 5.6-game series (60% of games OT) — wildly wrong vs reality (~20%).
      // Fix: resample wrong-direction games with a small budget. Independently roll pOT for OT tag.
      let wentOT = false;
      let retries = 0;
      while (((homeWins && goalsH <= goalsA) || (!homeWins && goalsA <= goalsH)) && retries < 8) {
        goalsH = samplePoisson(lamH, rng);
        goalsA = samplePoisson(lamA, rng);
        retries++;
      }
      // If still wrong after resampling (rare), fall back to bump (and tag as OT honestly).
      if (homeWins && goalsH <= goalsA) { goalsH = goalsA + 1; wentOT = true; }
      else if (!homeWins && goalsA <= goalsH) { goalsA = goalsH + 1; wentOT = true; }
      // Apply OT tag using the game's pOT as a probability on close games where margin === 1.
      // (Independent of the resample loop above — natural 1-goal games can go to OT.)
      if (!wentOT && Math.abs(goalsH - goalsA) === 1) {
        if (rng() < (g.pOT || 0.22)) wentOT = true;
      }
      trialGoalsH += goalsH;
      trialGoalsA += goalsA;
      if (goalsH === 0 || goalsA === 0) trialShutouts++;
      if (wentOT) trialOT++;

      // === L1: distribute team goals across team skaters weighted by per-game goal lambda ===
      // v57: track WHICH players scored each goal so we can boost their linemates' assist weights.
      const homeScorers = [];  // pool indices, one entry per goal scored
      const awayScorers = [];
      // Home goals
      if (goalsH > 0) {
        const weights = new Array(homeIdx.length);
        let wt = 0;
        for (let j = 0; j < homeIdx.length; j++) {
          const w = pool[homeIdx[j]].perStat.g.perGameFuture[gi];
          weights[j] = w; wt += w;
        }
        if (wt > 0) {
          const draws = multinomialDraws(goalsH, weights, wt, rng);
          for (let j = 0; j < homeIdx.length; j++) {
            trialTotals[homeIdx[j]].g += draws[j];
            for (let k = 0; k < draws[j]; k++) homeScorers.push(homeIdx[j]);
          }
        }
      }
      // Away goals
      if (goalsA > 0) {
        const weights = new Array(awayIdx.length);
        let wt = 0;
        for (let j = 0; j < awayIdx.length; j++) {
          const w = pool[awayIdx[j]].perStat.g.perGameFuture[gi];
          weights[j] = w; wt += w;
        }
        if (wt > 0) {
          const draws = multinomialDraws(goalsA, weights, wt, rng);
          for (let j = 0; j < awayIdx.length; j++) {
            trialTotals[awayIdx[j]].g += draws[j];
            for (let k = 0; k < draws[j]; k++) awayScorers.push(awayIdx[j]);
          }
        }
      }
      // === Assists: NHL avg ~1.67 assists per goal at even strength, higher on PP. Use 1.7.
      // v57: per-goal assist draw with linemate boost. For each goal:
      //   1. Determine 0/1/2 assists from distribution
      //   2. Build assist-weight array using base assist lambda
      //   3. Boost linemates of the scorer by LINEMATE_BOOST (3x)
      //   4. Exclude scorer from the assist pool (can't assist own goal)
      //   5. Sample assist recipient(s)
      // If no linemate map, this still works correctly with uniform boost (just slightly slower than the old loop).
      const assistPerGoalDist = [0.15, 0.50, 0.35]; // P(0 assists, 1 assist, 2 assists)
      for (let side = 0; side < 2; side++) {
        const scorers = side === 0 ? homeScorers : awayScorers;
        if (scorers.length === 0) continue;
        const idxArr = side === 0 ? homeIdx : awayIdx;
        // Base assist weights from per-game assist lambda
        const baseWeights = new Array(idxArr.length);
        let baseWt = 0;
        for (let j = 0; j < idxArr.length; j++) {
          const w = pool[idxArr[j]].perStat.a.perGameFuture[gi];
          baseWeights[j] = w; baseWt += w;
        }
        if (baseWt <= 0) continue;
        // For each goal, build a possibly-modified weights array
        for (let goalK = 0; goalK < scorers.length; goalK++) {
          const scorerPoolIdx = scorers[goalK];
          const u = rng();
          let numAssists = 0;
          if (u < assistPerGoalDist[0]) numAssists = 0;
          else if (u < assistPerGoalDist[0] + assistPerGoalDist[1]) numAssists = 1;
          else numAssists = 2;
          if (numAssists === 0) continue;
          // Build per-goal weight array: copy base, exclude scorer, boost linemates
          const w = new Array(idxArr.length);
          let wTot = 0;
          const matesOfScorer = (linemateIdx && linemateIdx[scorerPoolIdx]) || [];
          const mateSet = new Set(matesOfScorer);
          for (let j = 0; j < idxArr.length; j++) {
            const poolIdx = idxArr[j];
            if (poolIdx === scorerPoolIdx) { w[j] = 0; continue; }  // can't assist own goal
            let weight = baseWeights[j];
            if (mateSet.has(poolIdx)) weight *= LINEMATE_BOOST;
            w[j] = weight;
            wTot += weight;
          }
          if (wTot <= 0) continue;
          // v61 bug fix: 2nd assist must exclude the 1st assist recipient (same player can't
          //              have both A1 and A2 on a single goal). Previously we sampled twice
          //              from the same weight array, allowing double-attribution.
          let firstAssist = -1;
          for (let an = 0; an < numAssists; an++) {
            const pickJ = sampleCategorical(w, wTot, rng);
            trialTotals[idxArr[pickJ]].a += 1;
            if (an === 0 && numAssists === 2) {
              // Zero out this player for the second draw
              firstAssist = pickJ;
              wTot -= w[pickJ];
              w[pickJ] = 0;
              if (wTot <= 0) break;
            }
          }
        }
      }

      // === Non-scoring stats + individual SOG ===
      // SOG is "scoring" (correlates with goals-for environment), but we sample it per-player as NB since per-game SOG variance is substantial and multinomial doesn't apply (total SOG not constrained).
      for (let i = 0; i < N; i++) {
        const p = pool[i];
        // SOG
        {
          const mu = p.perStat.sog.perGameFuture[gi];
          if (mu > 0) trialTotals[i].sog += sampleNB(mu, r, rng);
        }
        // Hit, Blk, Tk, Pim, Give
        for (const stat of ["hit","blk","tk","pim","give"]) {
          const mu = p.perStat[stat].perGameFuture[gi];
          if (mu > 0) trialTotals[i][stat] += sampleNB(mu, r, rng);
        }
      }
    }

    // Tally series result
    const winner = hw >= 4 ? "H" : (aw >= 4 ? "A" : null);
    if (winner) {
      winnerCounts[winner]++;
      const gamesPlayed = hw + aw;
      seriesLenCounts[gamesPlayed]++;
      const key = `${hw}-${aw}`;
      exactScoreCounts[key] = (exactScoreCounts[key] || 0) + 1;
    }
    // Aggregate goal/shutout/OT histograms
    const seriesGoals = trialGoalsH + trialGoalsA;
    if (seriesGoals < seriesGoalsHist.length) seriesGoalsHist[seriesGoals]++;
    if (trialGoalsH < homeGoalsHist.length) homeGoalsHist[trialGoalsH]++;
    if (trialGoalsA < awayGoalsHist.length) awayGoalsHist[trialGoalsA]++;
    if (trialShutouts < seriesShutoutsHist.length) seriesShutoutsHist[trialShutouts]++;
    if (trialOT < seriesOTHist.length) seriesOTHist[trialOT]++;
    totalShutouts += trialShutouts;
    totalOT += trialOT;

    // Per-player totals into histograms
    for (let i = 0; i < N; i++) {
      const tt = trialTotals[i], ph = playerHist[i];
      if (tt.g <= KMAX) ph.g[tt.g]++;
      if (tt.a <= KMAX) ph.a[tt.a]++;
      if (tt.sog <= KMAX) ph.sog[tt.sog]++;
      if (tt.hit <= KMAX) ph.hit[tt.hit]++;
      if (tt.blk <= KMAX) ph.blk[tt.blk]++;
      if (tt.tk <= KMAX) ph.tk[tt.tk]++;
      if (tt.pim <= KMAX) ph.pim[tt.pim]++;
      if (tt.give <= KMAX) ph.give[tt.give]++;
    }
    // Leader market credits (fractional on ties)
    for (const stat of ["g","a","pts","sog","hit","blk","tk","pim","give"]) {
      let maxVal = -1, tiedCount = 0;
      for (let i = 0; i < N; i++) {
        const v = stat === "pts" ? (trialTotals[i].g + trialTotals[i].a) : trialTotals[i][stat];
        if (v > maxVal) { maxVal = v; tiedCount = 1; }
        else if (v === maxVal) tiedCount++;
      }
      const credit = 1 / tiedCount;
      for (let i = 0; i < N; i++) {
        const v = stat === "pts" ? (trialTotals[i].g + trialTotals[i].a) : trialTotals[i][stat];
        if (v === maxVal) leaderWins[stat][i] += credit;
      }
    }
  }

  // Normalize
  const norm = (arr) => arr.map(c => c / trials);
  return {
    trials,
    pool: pool.map(p => ({name: p.name, team: p.team})),
    // Series outcomes
    winnerProb: { H: winnerCounts.H / trials, A: winnerCounts.A / trials },
    seriesLengthProb: { 4: seriesLenCounts[4]/trials, 5: seriesLenCounts[5]/trials, 6: seriesLenCounts[6]/trials, 7: seriesLenCounts[7]/trials },
    exactScoreProb: Object.fromEntries(Object.entries(exactScoreCounts).map(([k,v])=>[k,v/trials])),
    // Goals / shutouts / OT
    seriesGoalsPMF: norm(seriesGoalsHist),
    homeGoalsPMF: norm(homeGoalsHist),
    awayGoalsPMF: norm(awayGoalsHist),
    seriesShutoutsPMF: norm(seriesShutoutsHist),
    seriesOTPMF: norm(seriesOTHist),
    avgShutouts: totalShutouts / trials,
    avgOT: totalOT / trials,
    // Per-player
    playerPMF: pool.map((p,i) => ({
      name: p.name, team: p.team,
      g: norm(playerHist[i].g),
      a: norm(playerHist[i].a),
      sog: norm(playerHist[i].sog),
      hit: norm(playerHist[i].hit),
      blk: norm(playerHist[i].blk),
      tk: norm(playerHist[i].tk),
      pim: norm(playerHist[i].pim),
      give: norm(playerHist[i].give),
    })),
    // Leader markets (true probs; caller applies margin)
    leaderProb: Object.fromEntries(Object.entries(leaderWins).map(([stat,arr])=>[stat, arr.map(c => c/trials)])),
  };
}

// Helpers to price O/U markets from a PMF
function pAtLeastFromPMF(pmf, k) {
  let s = 0;
  for (let i = k; i < pmf.length; i++) s += pmf[i];
  return s;
}
function pOverLineFromPMF(pmf, line) {
  // line typically x.5; strict > line means >= ceil(line+0.001)
  const k = Math.ceil(line - 0.001); // for 4.5 → 5, for 4.0 → 4
  // "Over" convention: >= k+1 when line is a half-line; >= k when line is integer... we use >=k+1 to be Over for x.5 lines
  // Actually: for line=4.5, Over = total>=5, k_ceil=5. pOver = P(X>=5) = pAtLeast(5).
  // for line=4.0, "Over" usually means P(X>=5) not P(X>=4). So always >= floor(line)+1 when line is integer; >= ceil(line) when half.
  const threshold = Math.floor(line) === line ? line + 1 : Math.ceil(line);
  return pAtLeastFromPMF(pmf, threshold);
}
function toAmer(p) {
  if (p <= 0.002) return 50000;
  // v49: cap at 0.9999 so normalized-with-overround values >= 1 don't produce positive nonsense
  const q = Math.min(0.9999, p);
  if (q >= 0.5) return -Math.round((q / (1 - q)) * 100);
  return Math.round(((1 - q) / q) * 100);
}
function toDec(p) {
  if (p <= 0.002) return 501;
  const q = Math.min(0.9999, p);
  return Math.min(501, +(1 / q).toFixed(2));
}

// v49: read a player's realized playoff total for a given stat key.
// Handles the "pts" case (not stored; derived from pG+pA) and normalizes field-name casing.
function readActual(p, stat, roundFilter) {
  if (!p) return 0;
  // v76: when roundFilter is passed (e.g. "r1", "r2"), sum only that round's per-game stats
  // from p.pGames. Without the filter, returns cumulative playoff totals (rollup fields).
  // Required because rollup fields (pG/pA/pSOG/etc.) accumulate across ALL rounds,
  // which makes "Now" columns wrong in per-series and R1-scope leader markets once R2 starts.
  if (roundFilter && p.pGames && roundFilter !== "full") {
    const roundNum = roundFilter === "r1" ? 1 : roundFilter === "r2" ? 2 :
                     roundFilter === "conf" ? 3 : roundFilter === "cup" ? 4 : null;
    if (roundNum != null) {
      let total = 0;
      for (const e of p.pGames) {
        if (e.round !== roundNum) continue;
        if (stat === "pts") total += (e.g||0) + (e.a||0);
        else if (stat === "tk") total += e.tk||0;
        else if (stat === "give") total += e.give||0;
        else if (stat === "pim") total += e.pim||0;
        else if (stat === "g") total += e.g||0;
        else if (stat === "a") total += e.a||0;
        else if (stat === "sog") total += e.sog||0;
        else if (stat === "hit") total += e.hit||0;
        else if (stat === "blk") total += e.blk||0;
        else if (stat === "toi") total += e.toi||0;
      }
      return total;
    }
  }
  if (stat === "pts") return (p.pG||0) + (p.pA||0);
  if (stat === "tk")  return p.pTK  || 0;
  if (stat === "give")return p.pGIVE|| p.pGV || 0;
  if (stat === "tsa") return p.pTSA || 0;
  if (stat === "pim") return p.pPIM || 0;
  if (stat === "g")   return p.pG   || 0;
  if (stat === "a")   return p.pA   || 0;
  if (stat === "sog") return p.pSOG || 0;
  if (stat === "hit") return p.pHIT || 0;
  if (stat === "blk") return p.pBLK || 0;
  if (stat === "toi") return p.pTOI || 0;  // v56 — playoff total TOI in minutes
  return 0;
}
// v76: games played by a player in a specific round (or all playoffs).
// Used so that "future games left in series" is calculated correctly per-round —
// otherwise an R2 player who played 5 R1 games has pGP=5 even before R2 G1.
function readActualGP(p, roundFilter) {
  if (!p) return 0;
  if (!roundFilter || roundFilter === "full" || !p.pGames) return p.pGP || 0;
  const roundNum = roundFilter === "r1" ? 1 : roundFilter === "r2" ? 2 :
                   roundFilter === "conf" ? 3 : roundFilter === "cup" ? 4 : null;
  if (roundNum == null) return p.pGP || 0;
  let gp = 0;
  for (const e of p.pGames) if (e.round === roundNum) gp++;
  return gp;
}

// Stats that scale with game total goals (offense-linked).
// Defensive stats (hits/blk/tk/pim/give) don't scale with scoring environment.
const SCORING_STATS = new Set(["g","a","pts","sog"]);
// v21: stats that INCREASE in playoffs (more physical, tighter play)
// We apply a DIFFERENT rate adjustment to these than scoring stats.
const PHYSICAL_STATS = new Set(["hit","blk","pim"]);
function goalScaleFor(stat, scale) {
  return SCORING_STATS.has(stat) ? (scale || 1) : 1;
}
// v21: stat-specific rate discount. Scoring drops ~15% in playoffs; physical rises ~5-10%; takeaways/giveaways roughly steady.
// Applied on top of user's global rateDiscount (which we interpret as the SCORING baseline now).
function statRateMultiplier(stat) {
  if (SCORING_STATS.has(stat)) return 1.0;   // user's rateDiscount applies as-is (typically 0.85)
  if (PHYSICAL_STATS.has(stat)) return 1.25; // physical stats ~25% higher relative to the user's scoring discount (so 0.85 * 1.25 ≈ 1.06 of regular-season rate)
  return 1.10; // takeaways/giveaways slightly up
}

// v46: stat-specific NB dispersion. Goals/assists/points over a 5-7 game window are very close to Poisson;
// heavy overdispersion (low r) wrongly inflates P(0) and crushes 1+/3+/5+ markets.
// Physical/discretionary stats (hits, blocks, PIM, give, take) have legitimate game-to-game variance and benefit from NB.
// Returns r (dispersion shape). User's globals.dispersion is treated as the PHYSICAL baseline.
function dispersionFor(stat, globalDispersion) {
  // Scoring stats: very mild overdispersion only. Cap at 8 (essentially Poisson) regardless of global setting.
  if (stat === "g" || stat === "a" || stat === "pts") return 12;
  // SOG: nearly Poisson; small overdispersion
  if (stat === "sog") return 10;
  // Physical / discretionary: use global setting (default 1.2 → heavy overdispersion is realistic here)
  return globalDispersion;
}

function applyMargin(trueProbs, or) {
  const s = trueProbs.reduce((a, b) => a + b, 0);
  // v64: apply per-outcome juice (rather than total-overround spread) in cases where
  //      the standard p/sum × OR formula misprices reduced markets.
  //      Two triggers, both meaning "this market is heavily concentrated / reduced":
  //        (a) Single surviving outcome — overround would push prob to OR > 1
  //        (b) One outcome > 50% — overround inflates the favorite past true prob
  //        (c) Market collapsed: <= 1/3 of original outcomes survive (e.g., series-clinch
  //            reduces a 70-way market to a single path; series partial-clinch reduces to
  //            a handful). Forces per-outcome juice rather than artificial concentration.
  //      Per-outcome juice: each surviving prob × (1 + edge), capped at 0.995.
  //      Aligns prices across markets that resolve to the same underlying event (length=N,
  //      exact-score, win-order matching path, spread covering one outcome).
  const surviving = trueProbs.filter(p => p > 0.001);
  const maxProb = Math.max(0, ...surviving);
  const heavyReduction = trueProbs.length > 0 && surviving.length > 0 && surviving.length <= Math.max(1, Math.ceil(trueProbs.length / 3));
  const useJuice = s > 0 && (surviving.length === 1 || maxProb > 0.50 || heavyReduction);
  if (useJuice) {
    const edge = Math.min(or - 1, 0.05);
    return trueProbs.map(p => p > 0.001 ? Math.min(0.995, p * (1 + edge)) : 0);
  }
  return trueProbs.map(p => s > 0 ? (p / s) * or : 0);
}

// v49: leader market overround — preserves favorite's true prob and squeezes longshots.
// Problem with naive (p/sum * or): if favorite has raw 0.7, OR=1.5 → 1.05 (impossible).
// Solution: cap each player at MAX_PROB, redistribute any overflow proportionally to the remaining field.
// This mimics how books actually juice leader markets: favorite moves a little, the tail pays the margin.
function applyLeaderOverround(rawProbs, powerFactor, overround, MAX_PROB = 0.95) {
  // v65: bumped MAX_PROB from 0.90 → 0.95. The cap exists to prevent impossible-probability outputs
  //      when overround pushes a favorite past 1.0, but 0.90 was too tight — high temp values would
  //      hit the cap and become "inert" (changing temp didn't move the favorite). 0.95 gives the
  //      knob more travel room while still preventing genuinely impossible probabilities.
  const pf = powerFactor || 1.0;
  const or = overround || 1.0;
  const powered = rawProbs.map(p => Math.pow(Math.max(0, p), pf));
  const psum = powered.reduce((a, b) => a + b, 0) || 1;
  // Initial allocation: (p/sum) * OR
  let adj = powered.map(p => (p / psum) * or);
  // Cap-and-redistribute: any player over MAX_PROB is clamped; overflow redistributes pro-rata across non-capped.
  // Iterate (up to 5 passes) because each redistribution may push another player over cap.
  for (let iter = 0; iter < 5; iter++) {
    let overflow = 0;
    const nonCappedSum = [];
    for (let i = 0; i < adj.length; i++) {
      if (adj[i] > MAX_PROB) {
        overflow += (adj[i] - MAX_PROB);
        adj[i] = MAX_PROB;
      } else {
        nonCappedSum.push(i);
      }
    }
    if (overflow < 1e-9) break;
    const nonSum = nonCappedSum.reduce((s, i) => s + adj[i], 0);
    if (nonSum <= 0) break;
    for (const i of nonCappedSum) adj[i] += (adj[i] / nonSum) * overflow;
  }
  return adj;
}

// v24: Team strength from on-ice xG. Sum across active (non-scratched) skaters.
// Since every on-ice event has ~5 skaters recorded, the sums overcount by ~5x,
// but since we take a ratio (xGF vs xGA) the scaling cancels.
// Returns { xGF60, xGA60, diff60, sampleGP } — per-60-minutes rates.
// If insufficient data, returns null.
function computeTeamStrength(players, team) {
  if (!players || !players.length) return null;
  const pool = players.filter(p => p.team === team && p.lineRole !== "SCRATCHED" && p.toi > 0);
  if (pool.length < 3) return null;
  let totF = 0, totA = 0, totTOI = 0;
  for (const p of pool) {
    totF += p.onIceF || 0;
    totA += p.onIceA || 0;
    totTOI += p.toi || 0;
  }
  if (totTOI <= 0) return null;
  // icetime is in seconds, convert to 60-min units
  const hours60 = totTOI / 3600;
  return {
    xGF60: totF / hours60,
    xGA60: totA / hours60,
    diff60: (totF - totA) / hours60,
    activeSkaters: pool.length,
  };
}

// Win probability from team strength differential.
// Calibration: NHL historical xG-diff vs win rate at 5v5 produces k ~= 0.15–0.22.
// Since we're using all-situations, slightly lower spread, so k=0.18 default.
// HFA applied as additive boost to home team's diff.
function winProbFromStrength(homeStrength, awayStrength, hfa = 0.05, k = 1.0) {
  if (!homeStrength || !awayStrength) return null;
  // diff is in xGF-xGA per 60. Typical span: -0.5 to +0.5.
  // hfa is expressed as a winPct bump: want a neutral-strength game with hfa=0.05 to give 0.55.
  // So convert hfa (in win%) to logit space and add to base logit.
  const edge = homeStrength.diff60 - awayStrength.diff60; // per-60 xG differential
  // Empirical: at 5v5 per-60, 1.0 xG/60 diff ~= 70% win prob in a single NHL game.
  // logit(0.70) = 0.847, so k_single_game ≈ 0.85 per unit of xG/60 diff.
  // Using all-situations (noisier than 5v5), dial down to 0.65 to avoid overconfidence
  // on the favorite. Calibrated so best-team vs worst-team (edge ~1.1 xG/60) gives ~67%,
  // matching typical market prices on top-vs-bottom playoff matchups.
  const baseLogit = 0.65 * edge * k;
  const hfaLogit = Math.log(hfa > 0 ? (0.5 + hfa) / (0.5 - hfa) : 1);
  const logit = baseLogit + hfaLogit;
  const p = 1 / (1 + Math.exp(-logit));
  return Math.max(0.05, Math.min(0.95, p));
}

// v21 distribution helpers
// Build a Poisson PMF truncated at maxK as a Float64Array (index = k)
function poissonPMFArray(lam, maxK) {
  const arr = new Array(maxK+1).fill(0);
  if (lam <= 0) { arr[0] = 1; return arr; }
  // Start from k=0 and iterate to avoid numerical issues
  let p = Math.exp(-lam);
  arr[0] = p;
  for (let k = 1; k <= maxK; k++) {
    p = p * lam / k;
    arr[k] = p;
  }
  return arr;
}
// Convolve two discrete distributions (arrays indexed by k), truncated at maxK.
// Used for summing independent Poisson variables (e.g., total goals across games).
function convolve(a, b, maxK) {
  const out = new Array(maxK+1).fill(0);
  const la = Math.min(a.length-1, maxK);
  const lb = Math.min(b.length-1, maxK);
  for (let i = 0; i <= la; i++) {
    if (a[i] === 0) continue;
    const cap = Math.min(lb, maxK-i);
    for (let j = 0; j <= cap; j++) {
      out[i+j] += a[i] * b[j];
    }
  }
  return out;
}
// Mix two PMF arrays with weights wA, wB (wA+wB should =1)
function mixPMF(a, b, wA, wB, maxK) {
  const out = new Array(maxK+1).fill(0);
  for (let k = 0; k <= maxK; k++) {
    out[k] = (a[k]||0)*wA + (b[k]||0)*wB;
  }
  return out;
}
// Scale PMF by a scalar weight (for path mixing)
function scalePMF(a, w, maxK) {
  const out = new Array(maxK+1).fill(0);
  const la = Math.min(a.length-1, maxK);
  for (let k = 0; k <= la; k++) out[k] = a[k]*w;
  return out;
}
// Add two PMF arrays (without renormalization — used as mixture accumulator)
function addPMF(a, b, maxK) {
  const out = new Array(maxK+1).fill(0);
  for (let k = 0; k <= maxK; k++) out[k] = (a[k]||0) + (b[k]||0);
  return out;
}
// P(X >= k) from a PMF array
function pAtLeast(pmf, k) {
  let s = 0; for (let i = k; i < pmf.length; i++) s += pmf[i]; return Math.min(1, s);
}
// O/U from arbitrary PMF (line is half-integer)
function ouFromPMF(pmf, lines) {
  return lines.map(line => {
    const k = Math.ceil(line);
    const pOver = pAtLeast(pmf, k);
    return { line, pOver, pUnder: 1 - pOver };
  });
}

// v13 Bayesian shrinkage for per-game rates — used by all prop/leader panels.
// Prevents 1-GP call-ups (e.g., Booth, Rooney, Luneau) from showing λ≈5 in R1 goals leader.
// Only applies to SMALL samples (gp < 20). Veterans with 20+ GP use their raw rate.
// Scratched players are excluded upstream by roleMultiplier=0, so no need to check here.
// shrunkRate = (gp*rawRate + k*prior) / (gp + k), with k=20 game-equivalent prior weight.
// stat = "g"|"a"|"pts"|"sog"|"hit"|"blk"|"tk"|"pim"|"give"
const PRIOR_RATES = {g:0.1, a:0.18, pts:0.28, sog:1.3, hit:1.0, blk:0.7, tk:0.4, pim:0.3, give:0.4};
// v103: lowered shrinkage. K=20/threshold=20 was over-penalizing rookies and recent call-ups
//       (e.g. Martone with 9 GP @ 0.44 g/g being shrunk to 0.21). Lower K + threshold give
//       small samples more weight while still pulling truly tiny samples toward the prior.
const SHRINK_K = 10;
const SHRINK_THRESHOLD_GP = 15;
function shrinkRate(rawRate, gp, stat) {
  const prior = PRIOR_RATES[stat] ?? 0.1;
  if (!gp || gp <= 0) return prior;
  // Veterans (gp >= 20): use raw rate untouched
  if (gp >= SHRINK_THRESHOLD_GP) return rawRate || 0;
  // Small sample: shrink toward prior
  return (gp * (rawRate||0) + SHRINK_K * prior) / (gp + SHRINK_K);
}
// v66: recent-form blending. Blends regular-season rate with realized playoff per-game rate,
// weighted by playoff sample size.
// v67: cap raised 0.30 → 0.50, slope raised 0.05 → 0.10. At pGP=5 → w=0.50 cap.
// v68: SCOPED TO SERIES LEADER ONLY. v67 inadvertently bled into Props / Player Detail / Sim,
// breaking N+ market prices (Kapanen 4+ at -676 vs FD -115; Faber 6+ at +819 vs FD +10000).
// Goal of pricer is to MATCH market consensus (operating as a competitor book),
// not to disagree. Props/Sim/PlayerDetail revert to pure shrunk season rate.
// Formula: effective = (1-w) × season + w × playoff, w = min(0.50, pGP × 0.10).
// Only applies to scoring stats (g/a/sog) where reg→playoff signal transfers cleanly.
const BLEND_STATS = new Set(["g","a","sog"]);
// v99: round-aware blend. Previously used cumulative p.pG / p.pGP regardless of which
// round's market we were pricing. For R2 pricing, that meant a player's R1 hot stretch
// (e.g. Couturier 5G in 5 R1 games → 1.0 g/g rate) was blended into his R2 rate at 50% weight,
// making him an artificial favorite in R2 leader markets. Fix: filter pGames by `scope`.
//   scope = "r1" | "r2" | "conf" | "cup" | "full" (or null/undefined → cumulative for back-compat)
function blendedRate(p, stat, seasonRate, scope) {
  if (!p || !BLEND_STATS.has(stat)) return seasonRate;
  let pGP, poTotal;
  if (scope && scope !== "full" && Array.isArray(p.pGames)) {
    const roundNum = scope === "r1" ? 1 : scope === "r2" ? 2 : scope === "conf" ? 3 : scope === "cup" ? 4 : null;
    if (roundNum != null) {
      pGP = 0; poTotal = 0;
      for (const e of p.pGames) {
        if (e.round !== roundNum) continue;
        pGP++;
        if (stat === "g") poTotal += e.g||0;
        else if (stat === "a") poTotal += e.a||0;
        else if (stat === "sog") poTotal += e.sog||0;
      }
    } else {
      pGP = p.pGP || 0;
      poTotal = stat==="g" ? (p.pG||0) : stat==="a" ? (p.pA||0) : (p.pSOG||0);
    }
  } else {
    pGP = p.pGP || 0;
    poTotal = stat==="g" ? (p.pG||0) : stat==="a" ? (p.pA||0) : (p.pSOG||0);
  }
  if (pGP <= 0) return seasonRate;
  const poRate = poTotal / pGP;
  const w = Math.min(0.50, pGP * 0.10);
  return (1 - w) * seasonRate + w * poRate;
}
function fmt(p) { const a = toAmer(p); return a > 0 ? `+${a}` : `${a}`; }

// v20: per-game stats history.
// Each player has `pGames`: array of {round, game, g, a, sog, hit, blk, tk, pim, give}
// Rollups pGP/pG/pA/pSOG/pHIT/pBLK/pTK/pPIM/pGIVE are derived from this array.
// Migration: if player has pGP>0 but no pGames, synthesize a single R1-G1 entry lumping all stats.
function rollupFromGames(pGames) {
  const r = {pGP:0, pG:0, pA:0, pSOG:0, pHIT:0, pBLK:0, pTK:0, pPIM:0, pGIVE:0};
  if (!Array.isArray(pGames)) return r;
  for (const e of pGames) {
    r.pGP++;
    r.pG    += e.g||0;
    r.pA    += e.a||0;
    r.pSOG  += e.sog||0;
    r.pHIT  += e.hit||0;
    r.pBLK  += e.blk||0;
    r.pTK   += e.tk||0;
    r.pPIM  += e.pim||0;
    r.pGIVE += e.give||0;
  }
  return r;
}
// Returns a player object with rollup fields recomputed from pGames
function withRollups(p) {
  if (!p.pGames) return p;
  const r = rollupFromGames(p.pGames);
  return {...p, ...r};
}
// Migrate legacy player (pGP > 0 but no pGames) to new structure
function migratePlayer(p) {
  // v91: migrate legacy role labels on every read.
  // ON_ROSTER → ACTIVE; SCRATCHED/INACTIVE → IR (per user spec).
  let migrated = p;
  if (p.lineRole && p.lineRole !== canonicalRole(p.lineRole)) {
    migrated = {...p, lineRole: canonicalRole(p.lineRole)};
  }
  if (migrated.pGames || !(migrated.pGP > 0)) return migrated;
  // Collapse existing totals into a single synthetic R1-G1 entry
  const synthetic = {
    round:1, game:1,
    g: migrated.pG||0, a: migrated.pA||0, sog: migrated.pSOG||0,
    hit: migrated.pHIT||0, blk: migrated.pBLK||0, tk: migrated.pTK||0,
    pim: migrated.pPIM||0, give: migrated.pGIVE||0,
    _migrated: true,
  };
  return {...migrated, pGames:[synthetic]};
}

// v28: goalie playoff rollups. Each goalie has `pGames`: [{round, game, ga, sa, sv, so, toi, dec}]
function rollupGoalie(pGames) {
  const r = {pGP:0, pSaves:0, pSA:0, pGA:0, pSO:0, pTOI:0, pW:0, pL:0};
  if (!Array.isArray(pGames)) return r;
  for (const e of pGames) {
    r.pGP++;
    r.pSaves += e.sv||0;
    r.pSA    += e.sa||0;
    r.pGA    += e.ga||0;
    r.pSO    += e.so||0;
    r.pTOI   += e.toi||0;
    if (e.dec === "W") r.pW++;
    else if (e.dec === "L") r.pL++;
  }
  return r;
}
function withGoalieRollups(g) {
  if (!g.pGames) return g;
  return {...g, ...rollupGoalie(g.pGames)};
}

// v28: full-page Hockey Reference box-score parser.
// Input: paste of the entire HR game page (header + scoring summary + both teams' skater tables
// + goalie tables + advanced tables). Output: structured game with both teams' stats + goalies.
//
// Detection strategy:
// - Find lines matching "<TeamName>" headers (e.g., "Pittsburgh Penguins")
// - The TWO header lines at the top of the page are the score banner: each team appears with
//   its score on the next non-empty line ("3", "1-0"). The team listed FIRST is the AWAY team
//   (HR convention: away team appears in top banner, home team second).
//   ↑ correction: actually HR shows away first, then home. We use arena/PPG Paints to confirm.
// - For player tables, we look for blocks like "<TeamName>" followed by a "Rk\tPlayer\t..." header.
// - Two skater tables (one per team), two goalie tables.
// - "Advanced" tables (one per team) provide HIT/BLK columns.
// v78: Natural Stat Trick game-report paste parser. Used as alternate to HR for morning-after
// uploads when HR hasn't refreshed yet. Returns same shape as parseHRFullPage so downstream
// commit logic is unchanged.
//
// NST paste format (per user's reference paste):
//   Header lines (in order, near top):
//     "Philadelphia Flyers @ Pittsburgh Penguins"      ← away @ home
//     "2026-04-27"                                      ← date
//     "2 - 3"                                           ← away score - home score
//     "Final" or "Final OT" or "Final SO"
//
//   Per-team skater section headers: "Penguins - Individual" / "Flyers - Individual"
//   Followed by a column header row starting with "Player\tPosition\tTOI\t..." then data rows.
//   Each skater row is TAB-separated; the columns we need:
//     Player, Position, TOI, Goals, Total Assists, [...], Total Points, Shots, [...],
//     PIM, [...], Giveaways, Takeaways, Hits, [...], Shots Blocked
//
//   After skaters, a "Goalies" sub-header then a goalie table:
//     Player, TOI, Shots Against, Saves, Goals Against, [...] SV%, GAA, [...]
function parseNSTGameReport(text) {
  const NAME_TO_ABBR = {
    "Anaheim Ducks":"ANA","Boston Bruins":"BOS","Buffalo Sabres":"BUF","Calgary Flames":"CGY",
    "Carolina Hurricanes":"CAR","Chicago Blackhawks":"CHI","Colorado Avalanche":"COL",
    "Columbus Blue Jackets":"CBJ","Dallas Stars":"DAL","Detroit Red Wings":"DET",
    "Edmonton Oilers":"EDM","Florida Panthers":"FLA","Los Angeles Kings":"LAK",
    "Minnesota Wild":"MIN","Montreal Canadiens":"MTL","Montréal Canadiens":"MTL",
    "Nashville Predators":"NSH","New Jersey Devils":"NJD","New York Islanders":"NYI",
    "New York Rangers":"NYR","Ottawa Senators":"OTT","Philadelphia Flyers":"PHI",
    "Pittsburgh Penguins":"PIT","San Jose Sharks":"SJS","Seattle Kraken":"SEA",
    "St. Louis Blues":"STL","St Louis Blues":"STL","Tampa Bay Lightning":"TBL",
    "Toronto Maple Leafs":"TOR","Utah Mammoth":"UTA","Utah Hockey Club":"UTA",
    "Vancouver Canucks":"VAN","Vegas Golden Knights":"VEG","Washington Capitals":"WSH",
    "Winnipeg Jets":"WPG"
  };
  // Map "Penguins" → "PIT" via short last-word lookup against NAME_TO_ABBR.
  const SHORT_TO_ABBR = {};
  for (const [full, abbr] of Object.entries(NAME_TO_ABBR)) {
    const last = full.split(" ").pop();
    SHORT_TO_ABBR[last] = abbr;
  }
  // Special cases / multi-word short names that the simple last-word logic misses:
  SHORT_TO_ABBR["Maple Leafs"] = "TOR";
  SHORT_TO_ABBR["Blue Jackets"] = "CBJ";
  SHORT_TO_ABBR["Red Wings"] = "DET";
  SHORT_TO_ABBR["Golden Knights"] = "VEG";

  const lines = text.split(/\r?\n/).map(l=>l.replace(/\s+$/,""));

  // ── 1. HEADER (away @ home) ────────────────────────────────────────────────
  // First line containing " @ " between two team-name strings.
  let awayName=null, homeName=null, awayAbbr=null, homeAbbr=null, dateISO=null;
  let awayScore=null, homeScore=null, ot=false, so=false;
  for (let i=0; i<Math.min(lines.length, 30); i++) {
    const L = lines[i].trim();
    const m = L.match(/^(.+?)\s+@\s+(.+)$/);
    if (m && NAME_TO_ABBR[m[1].trim()] && NAME_TO_ABBR[m[2].trim()]) {
      awayName = m[1].trim(); homeName = m[2].trim();
      awayAbbr = NAME_TO_ABBR[awayName]; homeAbbr = NAME_TO_ABBR[homeName];
      // Date should be 1-2 lines below
      for (let j=i+1; j<Math.min(i+5, lines.length); j++) {
        const d = lines[j].trim();
        if (/^\d{4}-\d{2}-\d{2}$/.test(d)) { dateISO = d; break; }
      }
      // Score: a line with "<n> - <n>" pattern within 5 lines after
      for (let j=i+1; j<Math.min(i+8, lines.length); j++) {
        const sm = lines[j].trim().match(/^(\d+)\s*-\s*(\d+)$/);
        if (sm) { awayScore = +sm[1]; homeScore = +sm[2]; break; }
      }
      // OT/SO marker on the "Final" line
      for (let j=i+1; j<Math.min(i+10, lines.length); j++) {
        const fl = lines[j].trim();
        if (fl.startsWith("Final")) {
          if (/OT/i.test(fl)) ot = true;
          if (/SO/i.test(fl)) so = true;
          break;
        }
      }
      break;
    }
  }
  if (!awayAbbr || !homeAbbr) return {error:"Could not find header row 'Away @ Home' with recognized team names. Make sure you copied from the top of the NST report."};
  if (awayScore == null) return {error:"Could not find score line (e.g. '2 - 3') near the header."};

  // ── 2. PARSE SKATER TABLES ─────────────────────────────────────────────────
  // Find each team's "<Short> - Individual" section. Within it, find the table header line
  // starting with "Player" + tab/whitespace + "Position", then read rows until a blank line
  // or a non-data line (e.g. "Goalies").
  function findIndividualSection(teamShortName) {
    for (let i=0; i<lines.length; i++) {
      const L = lines[i].trim();
      if (L === `${teamShortName} - Individual`) return i;
    }
    return -1;
  }
  function parseSkaterRows(startIdx) {
    // Find header row
    let headerIdx = -1;
    for (let i=startIdx; i<Math.min(startIdx+80, lines.length); i++) {
      const L = lines[i].trim();
      // Skip the "Skaters" sub-header if present
      if (/^Player\s+Position\s+TOI/.test(L) || /^Player\tPosition\tTOI/.test(L)) {
        headerIdx = i; break;
      }
    }
    if (headerIdx === -1) return {players:[], endLine:startIdx};
    // Parse header to find column indices we care about.
    // NST headers are tab-separated when copied from the table.
    const headerCols = lines[headerIdx].split(/\t/).map(s=>s.trim());
    // Fallback: if there's only 1 col, the paste may be space-separated — try collapsing whitespace.
    const cols = headerCols.length >= 5 ? headerCols : lines[headerIdx].split(/\s{2,}|\t/).map(s=>s.trim());
    const idx = {};
    cols.forEach((h,i)=>{ idx[h]=i; });
    // Required columns
    const COL = {
      name: idx["Player"],
      pos: idx["Position"],
      toi: idx["TOI"],
      g: idx["Goals"],
      a: idx["Total Assists"],
      sog: idx["Shots"],
      pim: idx["PIM"],
      give: idx["Giveaways"],
      tk: idx["Takeaways"],
      hit: idx["Hits"],
      blk: idx["Shots Blocked"],
    };
    if (COL.name == null || COL.g == null) {
      return {players:[], endLine:headerIdx, _err:`NST skater header missing required columns. Found: ${cols.slice(0,15).join(" | ")}`};
    }
    const players = [];
    let i = headerIdx + 1;
    for (; i < lines.length; i++) {
      const raw = lines[i];
      if (!raw.trim()) break; // blank line ends table
      // Stop on next sub-header ("Goalies", "<Team> - On Ice", ...)
      if (/^Goalies\s*$/.test(raw.trim())) break;
      if (/ - (On Ice|Shift Report|Forward Lines|Linemates|Opposition|Individual Event Maps)\s*$/.test(raw.trim())) break;
      // Split row on tabs (preferred) or 2+ spaces
      const row = raw.includes("\t") ? raw.split(/\t/) : raw.split(/\s{2,}/);
      const name = (row[COL.name]||"").trim();
      if (!name) continue;
      // Skip aggregate rows like "Forwards" / "Defense"
      if (name === "Forwards" || name === "Defense" || name === "Defensemen") continue;
      const num = (j)=>{ if (j==null) return 0; const v=row[j]?.trim(); if (!v||v==="-") return 0; const n=parseFloat(v); return isFinite(n)?n:0; };
      const toi = (row[COL.toi]||"").trim() || "0:00";
      players.push({
        name,
        g: num(COL.g)|0,
        a: num(COL.a)|0,
        sog: num(COL.sog)|0,
        pim: num(COL.pim)|0,
        toi,
        hit: num(COL.hit)|0,
        blk: num(COL.blk)|0,
        tk: num(COL.tk)|0,
        give: num(COL.give)|0,
      });
    }
    return {players, endLine:i};
  }

  function parseGoalieRows(startIdx) {
    // Find "Goalies" sub-header, then header row starting with "Player".
    let headerIdx = -1;
    for (let i=startIdx; i<Math.min(startIdx+200, lines.length); i++) {
      const L = lines[i].trim();
      if (/^Player\s+TOI\s+Shots Against/.test(L) || /^Player\tTOI\tShots Against/.test(L)) {
        headerIdx = i; break;
      }
    }
    if (headerIdx === -1) return {goalies:[], endLine:startIdx};
    const headerCols = lines[headerIdx].split(/\t/).map(s=>s.trim());
    const cols = headerCols.length >= 5 ? headerCols : lines[headerIdx].split(/\s{2,}|\t/).map(s=>s.trim());
    const idx = {};
    cols.forEach((h,i)=>{ idx[h]=i; });
    const COL = {
      name: idx["Player"], toi: idx["TOI"],
      sa: idx["Shots Against"], sv: idx["Saves"], ga: idx["Goals Against"],
    };
    if (COL.name == null || COL.sa == null) return {goalies:[], endLine:headerIdx};
    const goalies = [];
    let i = headerIdx + 1;
    for (; i < lines.length; i++) {
      const raw = lines[i];
      if (!raw.trim()) break;
      if (/ - (On Ice|Shift Report|Forward Lines|Linemates|Opposition|Individual Event Maps|Individual)\s*$/.test(raw.trim())) break;
      // Stop on a new sub-header "Skaters" or "Goalies" (would mean we're past this table)
      if (/^(Skaters|Goalies)\s*$/.test(raw.trim())) break;
      const row = raw.includes("\t") ? raw.split(/\t/) : raw.split(/\s{2,}/);
      const name = (row[COL.name]||"").trim();
      if (!name) continue;
      // Skip what looks like another table's header repeated
      if (name === "Player") continue;
      const num = (j)=>{ if (j==null) return 0; const v=row[j]?.trim(); if (!v||v==="-") return 0; const n=parseFloat(v); return isFinite(n)?n:0; };
      // Sanity: TOI cell must look like mm:ss; if it's a position letter (D/R/L/C), this isn't a goalie row.
      const toiRaw = (row[COL.toi]||"").trim();
      if (!/^\d+:\d{2}$/.test(toiRaw)) continue;
      goalies.push({
        name,
        sa: num(COL.sa)|0,
        sv: num(COL.sv)|0,
        ga: num(COL.ga)|0,
        toi: toiRaw,
        dec:"", so:0,
      });
    }
    return {goalies, endLine:i};
  }

  // Identify each team's short name (last word of "<City> <Nickname>"):
  const homeShort = homeName.split(" ").pop();
  const awayShort = awayName.split(" ").pop();

  const teamData = {};
  for (const [abbr, short] of [[homeAbbr, homeShort], [awayAbbr, awayShort]]) {
    const sectionStart = findIndividualSection(short);
    if (sectionStart === -1) {
      teamData[abbr] = {players:[], goalies:[]};
      continue;
    }
    const sk = parseSkaterRows(sectionStart);
    const gl = parseGoalieRows(sk.endLine);
    teamData[abbr] = {players: sk.players, goalies: gl.goalies};
    if (sk._err) teamData[abbr]._err = sk._err;
  }

  if ((teamData[homeAbbr]?.players.length||0) === 0 && (teamData[awayAbbr]?.players.length||0) === 0) {
    const dbg = teamData[homeAbbr]?._err || teamData[awayAbbr]?._err || "";
    return {error: `Could not parse skater tables for either team. ${dbg}`};
  }

  return {
    awayAbbr, homeAbbr,
    awayName, homeName,
    awayScore, homeScore,
    ot, so, dateISO,
    awayPlayers: teamData[awayAbbr]?.players || [],
    homePlayers: teamData[homeAbbr]?.players || [],
    awayGoalies: teamData[awayAbbr]?.goalies || [],
    homeGoalies: teamData[homeAbbr]?.goalies || [],
  };
}


// v84: ESPN box score paste parser. Returns same shape as parseHRFullPage / parseNSTGameReport
// so downstream commit logic is unchanged.
//
// ESPN paste structure:
//   First team's name (sometimes prefixed with stray "a " or "h ")
//   "forwards" header
//   N rows alternating: "Player Name" then "#jersey"
//   "defensemen" header
//   M more "Player Name" / "#jersey" rows
//   "Time On Ice    Faceoffs"  (label row, ignore)
//   "G A +/- S SM BS PN PIM HT TK GV SHFT TOI PPTOI SHTOI ESTOI FW FL FO%"  (forwards header)
//   N forwards stat rows (tab-separated)
//   second header for defensemen, M defensemen stat rows
//   "goalies" header, names, header row, stat row(s)
//   Repeat entire pattern for second team
//   "Scoring Summary" — running score "Away Home" trails each goal line; final = last pair
function parseESPNBoxScore(text) {
  const NAME_TO_ABBR = {
    "Anaheim Ducks":"ANA","Boston Bruins":"BOS","Buffalo Sabres":"BUF","Calgary Flames":"CGY",
    "Carolina Hurricanes":"CAR","Chicago Blackhawks":"CHI","Colorado Avalanche":"COL",
    "Columbus Blue Jackets":"CBJ","Dallas Stars":"DAL","Detroit Red Wings":"DET",
    "Edmonton Oilers":"EDM","Florida Panthers":"FLA","Los Angeles Kings":"LAK",
    "Minnesota Wild":"MIN","Montreal Canadiens":"MTL","Montréal Canadiens":"MTL",
    "Nashville Predators":"NSH","New Jersey Devils":"NJD","New York Islanders":"NYI",
    "New York Rangers":"NYR","Ottawa Senators":"OTT","Philadelphia Flyers":"PHI",
    "Pittsburgh Penguins":"PIT","San Jose Sharks":"SJS","Seattle Kraken":"SEA",
    "St. Louis Blues":"STL","St Louis Blues":"STL","Tampa Bay Lightning":"TBL",
    "Toronto Maple Leafs":"TOR","Utah Mammoth":"UTA","Utah Hockey Club":"UTA",
    "Vancouver Canucks":"VAN","Vegas Golden Knights":"VEG","Washington Capitals":"WSH",
    "Winnipeg Jets":"WPG"
  };
  const SHORT_TO_FULL = {};
  for (const full of Object.keys(NAME_TO_ABBR)) {
    const last = full.split(" ").pop();
    if (!SHORT_TO_FULL[last]) SHORT_TO_FULL[last] = full;
  }
  SHORT_TO_FULL["Maple Leafs"] = "Toronto Maple Leafs";
  SHORT_TO_FULL["Blue Jackets"] = "Columbus Blue Jackets";
  SHORT_TO_FULL["Red Wings"] = "Detroit Red Wings";
  SHORT_TO_FULL["Golden Knights"] = "Vegas Golden Knights";

  const lines = text.split(/\r?\n/).map(l=>l.replace(/\s+$/,""));
  const cleanTeamLine = (s) => s.replace(/^(a|h)\s+/i,"").trim();

  // Find the two team headers (line followed within 3 lines by "forwards")
  const teamHeaders = [];
  for (let i=0; i<lines.length; i++) {
    const cleaned = cleanTeamLine(lines[i]);
    if (!cleaned) continue;
    const fullName = NAME_TO_ABBR[cleaned] ? cleaned : (SHORT_TO_FULL[cleaned] || null);
    if (!fullName) continue;
    let confirmed = false;
    for (let j=i+1; j<Math.min(i+4, lines.length); j++) {
      if (lines[j].trim().toLowerCase() === "forwards") { confirmed = true; break; }
    }
    if (!confirmed) continue;
    if (teamHeaders.length > 0 && teamHeaders[teamHeaders.length-1].abbr === NAME_TO_ABBR[fullName]) continue;
    teamHeaders.push({idx:i, name:fullName, abbr:NAME_TO_ABBR[fullName]});
    if (teamHeaders.length === 2) break;
  }
  if (teamHeaders.length < 2) {
    return {error:"Could not find two team sections. Expected '<Team Name>' followed by 'forwards' for each team."};
  }
  const [away, home] = teamHeaders;

  function parseTeamSection(startIdx, endIdx) {
    const lo = startIdx;
    const hi = endIdx == null ? lines.length : endIdx;
    const findIn = (label, from = lo) => {
      for (let i=from; i<hi; i++) if (lines[i].trim().toLowerCase() === label) return i;
      return -1;
    };
    const fwdIdx = findIn("forwards");
    const defIdx = findIn("defensemen", fwdIdx>=0?fwdIdx+1:lo);
    const goalieIdx = findIn("goalies", defIdx>=0?defIdx+1:lo);
    if (fwdIdx === -1 || defIdx === -1) return null;

    function readNames(fromExclusive, toExclusive) {
      const names = [];
      let i = fromExclusive + 1;
      while (i < toExclusive) {
        const nameLine = lines[i].trim();
        const jerseyLine = (i+1 < toExclusive) ? lines[i+1].trim() : "";
        if (!nameLine || nameLine.toLowerCase()==="defensemen" || nameLine.toLowerCase()==="goalies") break;
        if (/^Time On Ice\b/i.test(nameLine)) break;
        if (/^G\s+A\s+\+\/-/i.test(nameLine) || /^G\tA\t\+\/-/.test(nameLine)) break;
        if (/^#/.test(nameLine)) { i++; continue; }
        if (/^#\d+/.test(jerseyLine)) {
          names.push(nameLine);
          i += 2;
        } else {
          names.push(nameLine);
          i++;
        }
      }
      return names;
    }
    const fwdNames = readNames(fwdIdx, defIdx);
    const defNames = readNames(defIdx, goalieIdx >= 0 ? goalieIdx : hi);

    function findStatHeader(from) {
      // ESPN paste has each column header on its own line: "G", "A", "+/-", "S", "SM", "BS", ...
      // The first data row appears right after the last header token "FO%".
      // We detect the header by spotting the sequence "G", "A", "+/-" on consecutive lines.
      // (Defensive fallback also matches single-line space/tab-separated header in case ESPN format changes.)
      for (let i=from; i<hi; i++) {
        const L = lines[i].trim();
        if (L === "G" && i+2 < hi && lines[i+1].trim() === "A" && lines[i+2].trim() === "+/-") return i;
        // Single-line variant (defensive)
        if (/^G\s+A\s+\+\/-\s+S\s+SM\s+BS/i.test(L) || /^G\tA\t\+\/-\tS\tSM\tBS/.test(L)) return i;
      }
      return -1;
    }
    // Skip past the vertical header (or 1-line header) and return the index of the first data row.
    function skipHeaderToData(headerIdx) {
      const L = lines[headerIdx].trim();
      // Single-line case: data starts on next line
      if (/^G\s+A\s+\+\/-\s+S\s+SM/i.test(L) || /^G\tA\t\+\/-\tS\tSM/.test(L)) return headerIdx + 1;
      // Vertical case: scan until we hit "FO%" then return next line
      for (let i=headerIdx; i<hi; i++) {
        if (lines[i].trim() === "FO%") return i + 1;
      }
      // Fallback — assume 19 vertical lines (G..FO%) and skip them.
      return headerIdx + 19;
    }
    function parseStatRow(raw) {
      const row = raw.includes("\t") ? raw.split(/\t/) : raw.split(/\s+/);
      const num = (j)=>{ const v = row[j]?.trim(); if (!v||v==="-") return 0; const n = parseFloat(v); return isFinite(n)?n:0; };
      // Stat positions: 0:G 1:A 2:+/- 3:S 4:SM 5:BS 6:PN 7:PIM 8:HT 9:TK 10:GV 11:SHFT 12:TOI
      return {
        g:   num(0)|0,
        a:   num(1)|0,
        sog: num(3)|0,
        pim: num(7)|0,
        hit: num(8)|0,
        tk:  num(9)|0,
        give:num(10)|0,
        blk: num(5)|0,
        toi: (row[12]||"0:00").trim(),
      };
    }
    const h1 = findStatHeader(defIdx);
    if (h1 === -1) return null;
    const fwdStats = [];
    let i = skipHeaderToData(h1);
    while (fwdStats.length < fwdNames.length && i < hi) {
      const L = lines[i];
      if (!L.trim()) { i++; continue; }
      // Skip another header that snuck in (defensemen header before defenseman data)
      if (L.trim() === "G" || /^G\s+A\s+\+\/-/i.test(L.trim())) break;
      fwdStats.push(parseStatRow(L));
      i++;
    }
    const h2 = findStatHeader(i);
    if (h2 === -1) return null;
    const defStats = [];
    i = skipHeaderToData(h2);
    while (defStats.length < defNames.length && i < hi) {
      const L = lines[i];
      if (!L.trim()) { i++; continue; }
      if (L.trim() === "G" || /^G\s+A\s+\+\/-/i.test(L.trim())) break;
      defStats.push(parseStatRow(L));
      i++;
    }

    const players = [];
    for (let j=0; j<fwdNames.length && j<fwdStats.length; j++) players.push({name:fwdNames[j], ...fwdStats[j]});
    for (let j=0; j<defNames.length && j<defStats.length; j++) players.push({name:defNames[j], ...defStats[j]});

    const goalies = [];
    if (goalieIdx !== -1) {
      // Header pattern: vertical ("SA" / "GA" / "SV" / "SV%" / ... / "PIM") OR single-line "SA  GA  SV  SV%..."
      const isGHeaderLine = (s) => s === "SA" || /^SA\s+GA\s+SV\s+SV%/i.test(s) || /^SA\tGA\tSV\tSV%/.test(s);
      const goalieNames = [];
      let gi = goalieIdx + 1;
      while (gi < hi && !isGHeaderLine(lines[gi].trim())) {
        const nm = lines[gi].trim();
        const jr = (gi+1 < hi) ? lines[gi+1].trim() : "";
        if (!nm) { gi++; continue; }
        if (/^#/.test(nm)) { gi++; continue; }
        if (/^#\d+/.test(jr)) { goalieNames.push(nm); gi += 2; }
        else { goalieNames.push(nm); gi++; }
      }
      if (gi < hi) {
        // Skip header lines. Vertical case = scan until we see "PIM"; single-line case = next line.
        let dataStart;
        const headerLine = lines[gi].trim();
        if (/^SA\s+GA\s+SV/i.test(headerLine) || /^SA\tGA\tSV/.test(headerLine)) {
          dataStart = gi + 1;
        } else {
          // Vertical — scan to "PIM"
          let k = gi;
          while (k < hi && lines[k].trim() !== "PIM") k++;
          dataStart = k + 1;
        }
        let si = dataStart;
        for (const gname of goalieNames) {
          while (si < hi && !lines[si].trim()) si++;
          if (si >= hi) break;
          const row = lines[si].includes("\t") ? lines[si].split(/\t/) : lines[si].split(/\s+/);
          const num = (j)=>{ const v = row[j]?.trim(); if (!v||v==="-") return 0; const n = parseFloat(v); return isFinite(n)?n:0; };
          goalies.push({
            name: gname,
            sa: num(0)|0,
            ga: num(1)|0,
            sv: num(2)|0,
            toi: (row[9]||"0:00").trim(),
            dec: "", so: 0,
          });
          si++;
        }
      }
    }
    return {players, goalies};
  }

  const scoringIdx = (()=>{
    for (let i=0; i<lines.length; i++) if (/^Scoring Summary\s*$/i.test(lines[i].trim())) return i;
    return lines.length;
  })();

  const awayData = parseTeamSection(away.idx, home.idx);
  const homeData = parseTeamSection(home.idx, scoringIdx);

  if (!awayData || !homeData) {
    return {error:"Could not parse one or both team sections. Need 'forwards', 'defensemen', and the stat header row 'G A +/- S SM BS ...' for each team."};
  }
  if (awayData.players.length === 0 && homeData.players.length === 0) {
    return {error:"No player stat rows parsed. Stat rows must be tab-separated; copy directly from the ESPN page."};
  }

  // Score: scan the scoring summary for trailing "<away> <home>" running-total pairs; final = last.
  let awayScore = 0, homeScore = 0;
  for (let i=scoringIdx; i<lines.length; i++) {
    const L = lines[i].trim();
    const m = L.match(/(?:^|\s)(\d+)[\s\t]+(\d+)\s*$/);
    if (m) {
      const a = +m[1], h = +m[2];
      if (a <= 15 && h <= 15) { awayScore = a; homeScore = h; }
    }
  }
  let ot = false, so = false;
  for (let i=scoringIdx; i<lines.length; i++) {
    if (/\bOT\d?\b/.test(lines[i])) ot = true;
    if (/\bShootout\b/i.test(lines[i])) so = true;
  }

  // v89: extract OT scorer when game went to OT.
  // ESPN scoring summary lines look like:  "2:45\t\tElmer Soderblom (1)"
  // (tabs may be inconsistent — "<MM:SS><whitespace+><Name> (<goalNum>)").
  // The last such line in the summary IS the OT winner when ot=true.
  let otScorer = null;
  if (ot) {
    let lastGoalLine = null;
    for (let i=scoringIdx; i<lines.length; i++) {
      const L = lines[i];
      // Match "<min>:<sec>" then whitespace then "<Name> (<num>)"
      const m = L.match(/^\s*\d+:\d{2}\s+(.+?)\s*\(\d+\)\s*$/);
      if (m) lastGoalLine = m[1].trim();
    }
    if (lastGoalLine) otScorer = lastGoalLine;
  }

  return {
    awayAbbr: away.abbr, homeAbbr: home.abbr,
    awayName: away.name, homeName: home.name,
    awayScore, homeScore,
    ot, so, otScorer, dateISO: null,
    awayPlayers: awayData.players,
    homePlayers: homeData.players,
    awayGoalies: awayData.goalies,
    homeGoalies: homeData.goalies,
  };
}


function parseHRFullPage(text) {
  const NAME_TO_ABBR = {
    "Anaheim Ducks":"ANA","Boston Bruins":"BOS","Buffalo Sabres":"BUF","Calgary Flames":"CGY",
    "Carolina Hurricanes":"CAR","Chicago Blackhawks":"CHI","Colorado Avalanche":"COL",
    "Columbus Blue Jackets":"CBJ","Dallas Stars":"DAL","Detroit Red Wings":"DET",
    "Edmonton Oilers":"EDM","Florida Panthers":"FLA","Los Angeles Kings":"LAK",
    "Minnesota Wild":"MIN","Montreal Canadiens":"MTL","Nashville Predators":"NSH",
    "New Jersey Devils":"NJD","New York Islanders":"NYI","New York Rangers":"NYR",
    "Ottawa Senators":"OTT","Philadelphia Flyers":"PHI","Pittsburgh Penguins":"PIT",
    "San Jose Sharks":"SJS","Seattle Kraken":"SEA","St. Louis Blues":"STL","St Louis Blues":"STL",
    "Tampa Bay Lightning":"TBL","Toronto Maple Leafs":"TOR","Utah Mammoth":"UTA","Utah Hockey Club":"UTA",
    "Vancouver Canucks":"VAN","Vegas Golden Knights":"VEG","Washington Capitals":"WSH",
    "Winnipeg Jets":"WPG"
  };

  const lines = text.split(/\r?\n/).map(l=>l.replace(/\s+$/,""));

  // ── 1. SCORE BANNER ─────────────────────────────────────────────────────────
  // First two team-name occurrences in the page = the score banner (away first, then home).
  // Each is followed by their score on the next non-empty line.
  const teamHits = []; // [{name, abbr, lineIdx}]
  for (let i=0; i<lines.length; i++) {
    const L = lines[i].trim();
    if (NAME_TO_ABBR[L]) {
      teamHits.push({name:L, abbr:NAME_TO_ABBR[L], lineIdx:i});
    }
  }
  if (teamHits.length < 2) return {error:"Could not find two team names. Paste the full HR page including score header."};

  // First two are score banner (away, home). Subsequent occurrences are section headers.
  const awayHit = teamHits[0], homeHit = teamHits[1];

  // Score = first numeric integer on a line shortly after the team name (within next 5 lines).
  const findScore = (start) => {
    for (let i=start+1; i<Math.min(start+6, lines.length); i++) {
      const L = lines[i].trim();
      if (/^\d+$/.test(L)) return parseInt(L);
    }
    return null;
  };
  const awayScore = findScore(awayHit.lineIdx);
  const homeScore = findScore(homeHit.lineIdx);
  if (awayScore == null || homeScore == null) return {error:"Could not parse final score from header banner."};

  // ── 2. OT/SO DETECTION ──────────────────────────────────────────────────────
  // Look for "OT" or "Overtime" or "SO"/"Shootout" or "1st OT", "2nd OT" mentions.
  const fullText = text;
  const ot = /\b(overtime|1st OT|2nd OT|3rd OT|4th OT)\b/i.test(fullText)
          || /\bOT\b/.test(fullText.split("\n").slice(0,40).join(" ")); // OT mentioned in scoring summary
  const so = /\b(shootout|^SO$)/im.test(fullText);

  // ── 3. DATE ─────────────────────────────────────────────────────────────────
  let dateISO = null;
  const dm = fullText.match(/([A-Z][a-z]+\s+\d{1,2},\s+\d{4})/);
  if (dm) {
    const d = new Date(dm[1]);
    if (!isNaN(d)) dateISO = d.toISOString().slice(0,10);
  }

  // ── 4. PER-TEAM SKATER + GOALIE TABLE EXTRACTION ────────────────────────────
  // For each team (sections 3+, after the banner), parse the next skater table.
  // A skater table starts with a header row containing "Player" and ends at "TOTAL" line.
  // After the skater table comes the goalie table (header has "DEC" and "SV%").
  function parseSkaterTable(startLine) {
    // Find the next header row containing "Player"
    let hi = -1, headers = [];
    for (let i=startLine; i<lines.length; i++) {
      const cells = lines[i].split("\t").map(s=>s.trim());
      const cellsLow = cells.map(c=>c.toLowerCase());
      if (cellsLow.includes("player") && cellsLow.includes("g") && cellsLow.includes("a")) {
        hi = i; headers = cells; break;
      }
    }
    if (hi === -1) return null;
    const col = (alts)=>{for(const a of alts){const i=headers.findIndex(h=>h.toLowerCase()===a.toLowerCase());if(i!==-1)return i;}return -1;};
    const cm = {
      name:col(["Player","Skater"]),
      g:col(["G"]), a:col(["A"]), pim:col(["PIM"]),
      sog:col(["S","Shots","SOG"]),
      toi:col(["TOI"]),
    };
    const players = [];
    let endLine = hi;
    for (let i=hi+1; i<lines.length; i++) {
      const cells = lines[i].split("\t").map(s=>s.trim());
      const name = cm.name>=0 ? cells[cm.name] : "";
      if (!name) { endLine = i; break; }
      // v28: TOTAL row may have empty Rk, so "TOTAL" lands in cells[0] not in name slot.
      // Check the WHOLE line for "TOTAL" or "Team Totals" prefix.
      const lineTrim = lines[i].trim();
      if (/^TOTAL\b/i.test(lineTrim) || /Team Totals/i.test(lineTrim)) { endLine = i; break; }
      if (NAME_TO_ABBR[lineTrim]) { endLine = i; break; }
      // Also: if name slot is purely numeric (like "2" from a TOTAL row that shifted columns), skip
      if (/^\d+$/.test(name)) continue;
      // Skip rows that don't look like player rows (need at least a few numeric cells)
      if (cells.length < 5) continue;
      // Skip goalie rows that may sneak in (Vladař etc. appear in skater table on HR sometimes — but
      // they'll show up here with TOI ~60:00 and 0/0 shots; we keep them since they don't contribute
      // skater stats anyway, but mark with flag for filtering).
      const g = cm.g>=0 ? parseInt(cells[cm.g])||0 : 0;
      const a = cm.a>=0 ? parseInt(cells[cm.a])||0 : 0;
      const sog = cm.sog>=0 ? parseInt(cells[cm.sog])||0 : 0;
      const pim = cm.pim>=0 ? parseInt(cells[cm.pim])||0 : 0;
      // TOI parse "MM:SS" → seconds
      let toi = 0;
      if (cm.toi>=0 && cells[cm.toi]) {
        const m = cells[cm.toi].match(/(\d+):(\d+)/);
        if (m) toi = parseInt(m[1])*60 + parseInt(m[2]);
      }
      players.push({name, g, a, sog, pim, toi, hit:0, blk:0, tk:0, give:0});
    }
    return {players, endLine};
  }

  function parseGoalieTable(startLine) {
    // Goalie tables have headers including "DEC", "SV%", "SA"
    let hi = -1, headers = [];
    for (let i=startLine; i<Math.min(startLine+200, lines.length); i++) {
      const cells = lines[i].split("\t").map(s=>s.trim());
      const cellsLow = cells.map(c=>c.toLowerCase());
      // Goalie header: must include DEC and SV% (and Player)
      if (cellsLow.includes("player") && cellsLow.includes("dec") && cellsLow.some(c=>c==="sv%"||c==="sv")) {
        hi = i; headers = cells; break;
      }
      // Stop searching if we hit another team name (next section)
      const trimmed = lines[i].trim();
      if (NAME_TO_ABBR[trimmed]) break;
    }
    if (hi === -1) return {goalies:[], endLine:startLine};
    const col = (alts)=>{for(const a of alts){const i=headers.findIndex(h=>h.toLowerCase()===a.toLowerCase());if(i!==-1)return i;}return -1;};
    const cm = {
      name: col(["Player","Goalie"]),
      dec:  col(["DEC"]),
      ga:   col(["GA"]),
      sa:   col(["SA"]),
      sv:   col(["SV"]),
      svp:  col(["SV%"]),
      so:   col(["SO"]),
      pim:  col(["PIM"]),
      toi:  col(["TOI"]),
    };
    const goalies = [];
    let endLine = hi;
    for (let i=hi+1; i<lines.length; i++) {
      const cells = lines[i].split("\t").map(s=>s.trim());
      const name = cm.name>=0 ? cells[cm.name] : "";
      if (!name) { endLine = i; break; }
      if (/^TOTAL/i.test(name) || NAME_TO_ABBR[name.trim()]) { endLine = i; break; }
      if (cells.length < 4) continue;
      const dec = cm.dec>=0 ? cells[cm.dec] : "";
      const ga = cm.ga>=0 ? parseInt(cells[cm.ga])||0 : 0;
      const sa = cm.sa>=0 ? parseInt(cells[cm.sa])||0 : 0;
      const sv = cm.sv>=0 ? parseInt(cells[cm.sv])||0 : Math.max(0,sa-ga);
      const so = cm.so>=0 ? parseInt(cells[cm.so])||0 : 0;
      let toi = 0;
      if (cm.toi>=0 && cells[cm.toi]) {
        const m = cells[cm.toi].match(/(\d+):(\d+)/);
        if (m) toi = parseInt(m[1])*60 + parseInt(m[2]);
      }
      goalies.push({name, dec, ga, sa, sv, so, toi});
    }
    return {goalies, endLine};
  }

  function parseAdvancedTable(startLine) {
    // Advanced has header: Player iCF SAT-F SAT-A CF% CRel% ZSO ZSD oZS% HIT BLK
    let hi = -1, headers = [];
    for (let i=startLine; i<Math.min(startLine+500, lines.length); i++) {
      const cells = lines[i].split("\t").map(s=>s.trim());
      const cellsLow = cells.map(c=>c.toLowerCase());
      if (cellsLow.includes("player") && cellsLow.some(c=>c==="hit") && cellsLow.some(c=>c==="blk")) {
        hi = i; headers = cells; break;
      }
      const trimmed = lines[i].trim();
      if (NAME_TO_ABBR[trimmed] && i>startLine+1) break;
    }
    if (hi === -1) return {map:new Map(), endLine:startLine};
    const col = (alts)=>{for(const a of alts){const i=headers.findIndex(h=>h.toLowerCase()===a.toLowerCase());if(i!==-1)return i;}return -1;};
    const cm = {
      name: col(["Player"]),
      hit:  col(["HIT"]),
      blk:  col(["BLK"]),
    };
    const map = new Map();
    let endLine = hi;
    for (let i=hi+1; i<lines.length; i++) {
      const cells = lines[i].split("\t").map(s=>s.trim());
      const name = cm.name>=0 ? cells[cm.name] : "";
      if (!name) { endLine = i; break; }
      if (/^TOTAL/i.test(name) || NAME_TO_ABBR[name.trim()]) { endLine = i; break; }
      if (cells.length < 3) continue;
      const hit = cm.hit>=0 ? parseInt(cells[cm.hit])||0 : 0;
      const blk = cm.blk>=0 ? parseInt(cells[cm.blk])||0 : 0;
      map.set(name, {hit, blk});
    }
    return {map, endLine};
  }

  // ── 5. PARSE TEAM SECTIONS ──────────────────────────────────────────────────
  // After the banner, the page has team sections in a deterministic order.
  // We search forward starting after homeHit.lineIdx for the FIRST team section header
  // (which will be either away or home, depending on HR layout).
  // Strategy: find ALL team-name occurrences after lineIdx of homeHit. They mark section starts.
  // For each team, the next skater table belongs to that team.
  const sectionStarts = [];
  for (let i=homeHit.lineIdx+1; i<lines.length; i++) {
    const L = lines[i].trim();
    if (NAME_TO_ABBR[L]) sectionStarts.push({abbr:NAME_TO_ABBR[L], name:L, lineIdx:i});
  }

  // We expect 4 section headers per team (one for skater table, one for goalie table title region,
  // one for advanced) but in practice HR repeats the team name multiple times. We just need to find
  // the FIRST skater table after the FIRST occurrence of each team.
  const firstAway = sectionStarts.find(s=>s.abbr===awayHit.abbr);
  const firstHome = sectionStarts.find(s=>s.abbr===homeHit.abbr);
  if (!firstAway || !firstHome) return {error:`Could not find skater section for both teams (away=${awayHit.abbr}, home=${homeHit.abbr})`};

  // Parse in order: away first, then home (HR away usually appears first in body sections too).
  // But to be robust, sort by lineIdx and parse each in order.
  const order = [firstAway, firstHome].sort((a,b)=>a.lineIdx - b.lineIdx);

  const teamData = {};
  for (let k=0; k<order.length; k++) {
    const sec = order[k];
    const next = order[k+1] ? order[k+1].lineIdx : lines.length;
    // Skater table within [sec.lineIdx, next)
    const sk = parseSkaterTable(sec.lineIdx);
    if (!sk) { teamData[sec.abbr] = {players:[], goalies:[]}; continue; }
    const gl = parseGoalieTable(sk.endLine);
    teamData[sec.abbr] = {players: sk.players, goalies: gl.goalies, _skaterEnd: sk.endLine, _goalieEnd: gl.endLine};
  }

  // Advanced section markers: "Pittsburgh Penguins Advanced" appears as a section header.
  // Find these explicitly since they don't appear in NAME_TO_ABBR.
  const advancedSections = []; // [{abbr, lineIdx}]
  for (let i=0; i<lines.length; i++) {
    const L = lines[i].trim();
    for (const [name, abbr] of Object.entries(NAME_TO_ABBR)) {
      if (L === name + " Advanced") {
        advancedSections.push({abbr, lineIdx:i});
        break;
      }
    }
  }

  // ── 6. ADVANCED TABLES (HIT/BLK) ────────────────────────────────────────────
  for (const sec of advancedSections) {
    const adv = parseAdvancedTable(sec.lineIdx);
    if (adv.map.size === 0) continue;
    for (const p of (teamData[sec.abbr]?.players || [])) {
      const stats = adv.map.get(p.name);
      if (stats) { p.hit = stats.hit; p.blk = stats.blk; }
    }
  }

  // v89: best-effort OT scorer extraction from HR scoring summary.
  // HR pages typically have a "Scoring Summary" or similar section with goal lines like:
  //   "OT  - 0:42 - Mark Stone — Tomas Hertl, Shea Theodore"
  // or:
  //   "1st OT  Mark Stone (1)"
  // Fallback: if game went to OT, scan after any "OT" period marker and grab the first
  // "Name (goalNum)" pattern. If not found, leave null — user can set manually.
  let otScorer = null;
  if (ot) {
    let foundOTSection = false;
    for (let i=0; i<lines.length; i++) {
      const L = lines[i];
      // OT period header (HR uses "OT", "1st OT", etc.)
      if (/^\s*(1st\s+|2nd\s+|3rd\s+|4th\s+)?OT\b/i.test(L) && !/^\s*OT\s*[:|]/.test(L)) {
        foundOTSection = true;
        // Look ahead 1-10 lines for "Name (goalNum)" pattern
        for (let j=i; j<Math.min(i+10, lines.length); j++) {
          const m = lines[j].match(/([A-ZÁÄÉÍÓÖÚÜ][a-záäéíóöúüñç'\.\-]+(?:\s+[A-ZÁÄÉÍÓÖÚÜ][a-záäéíóöúüñç'\.\-]+)+)\s*\(\d+\)/);
          if (m) { otScorer = m[1].trim(); break; }
        }
        if (otScorer) break;
      }
    }
    // Fallback: HR's "Scoring Summary" block may format OT differently — try last-goal-of-the-game heuristic.
    if (!otScorer && foundOTSection === false) {
      let lastName = null;
      for (let i=0; i<lines.length; i++) {
        const m = lines[i].match(/^\s*\d+:\d{2}\s+(.+?)\s*\(\d+\)/);
        if (m) lastName = m[1].trim();
      }
      if (lastName) otScorer = lastName;
    }
  }

  return {
    awayAbbr: awayHit.abbr, homeAbbr: homeHit.abbr,
    awayName: awayHit.name, homeName: homeHit.name,
    awayScore, homeScore,
    ot, so, otScorer, dateISO,
    awayPlayers: teamData[awayHit.abbr]?.players || [],
    homePlayers: teamData[homeHit.abbr]?.players || [],
    awayGoalies: teamData[awayHit.abbr]?.goalies || [],
    homeGoalies: teamData[homeHit.abbr]?.goalies || [],
  };
}


// v57: Parse MoneyPuck lines.csv → linemates map for assist correlation.
// Input: CSV text with header row. Columns used: name (hyphenated line/pair), team, position, situation, icetime.
// Output: { "normLastName|team": [{mate: "normLastName", sharedTOI}, ...] } — top 3 by shared TOI per player.
// NOTE: MoneyPuck uses LAST names only (e.g., "Hyman-Mcdavid-Draisaitl"). We match by last-name + team.
//       Collisions (two players same last name on same team) are rare but possible — warn on match failure downstream.
function parseLinesCsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return {};
  const header = lines[0].split(",").map(s => s.trim());
  const col = (name) => header.indexOf(name);
  const iName = col("name"), iTeam = col("team"), iPos = col("position"), iSit = col("situation"), iIce = col("icetime");
  if (iName < 0 || iTeam < 0 || iIce < 0) return {};
  // Shared-TOI aggregation: for each (playerA, playerB) pair, sum icetime across all line/pairing rows they appear in together.
  // Keyed by `normLastA|team` for lookup.
  const pairTOI = {};  // { "a|team": { "b": cumulativeTOI } }
  for (let i = 1; i < lines.length; i++) {
    const c = lines[i].split(",");
    const lineName = (c[iName]||"").trim();
    const team = (c[iTeam]||"").trim();
    const pos = (c[iPos]||"").trim();
    const sit = (c[iSit]||"").trim();
    const ice = parseFloat(c[iIce]) || 0;
    if (!lineName || !team || ice <= 0) continue;
    // Only use 5on5 line combos (PP/PK have different dynamics, pairings shouldn't boost forward assists)
    if (sit !== "5on5") continue;
    if (pos !== "line") continue;  // forward lines only — defense pairings don't carry same assist-correlation
    // Split hyphenated names; normalize each to lowercase
    const names = lineName.split("-").map(s => s.trim().toLowerCase()).filter(Boolean);
    if (names.length < 2) continue;
    // All pairwise contributions
    for (let a = 0; a < names.length; a++) {
      for (let b = 0; b < names.length; b++) {
        if (a === b) continue;
        const keyA = names[a] + "|" + team;
        if (!pairTOI[keyA]) pairTOI[keyA] = {};
        pairTOI[keyA][names[b]] = (pairTOI[keyA][names[b]] || 0) + ice;
      }
    }
  }
  // Convert to top-3 lists
  const linemates = {};
  for (const [key, matesObj] of Object.entries(pairTOI)) {
    const mates = Object.entries(matesObj)
      .map(([mate, sharedTOI]) => ({mate, sharedTOI}))
      .sort((x, y) => y.sharedTOI - x.sharedTOI)
      .slice(0, 3);
    linemates[key] = mates;
  }
  return linemates;
}
function computeOutcomes(games) {
  const agg = {};
  function rec(gi, hw, aw, prob) {
    if (hw === 4 || aw === 4 || gi >= 7) { const k=`${hw}-${aw}`; agg[k]=(agg[k]||0)+prob; return; }
    const g = games[gi];
    if (g.result === "home") rec(gi+1, hw+1, aw, prob);
    else if (g.result === "away") rec(gi+1, hw, aw+1, prob);
    else { rec(gi+1, hw+1, aw, prob*g.winPct); rec(gi+1, hw, aw+1, prob*(1-g.winPct)); }
  }
  rec(0, 0, 0, 1); return agg;
}
function computeWinOrders(games) {
  const seqs = {};
  function rec(gi, hw, aw, prob, seq) {
    if (hw===4||aw===4) { seqs[seq]=(seqs[seq]||0)+prob; return; }
    if (gi>=7) return;
    const g = games[gi];
    if (g.result==="home") rec(gi+1,hw+1,aw,prob,seq+"H");
    else if (g.result==="away") rec(gi+1,hw,aw+1,prob,seq+"A");
    else { rec(gi+1,hw+1,aw,prob*g.winPct,seq+"H"); rec(gi+1,hw,aw+1,prob*(1-g.winPct),seq+"A"); }
  }
  rec(0,0,0,1,""); return seqs;
}
// v22: P(game goes to OT) = P(regulation ends tied), derived from expTotal and winPct.
// Pure Poisson underpredicts OT rate vs historical (real NHL playoff OT ~22%; naive model ~17%).
// Empirical calibration factor of 1.28 brings the model in line with historical averages —
// accounts for non-Poisson tie-clustering (teams play for the tie late in close games).
// Cap at 0.40.
function pOTGame(expTotal, winPct) {
  const regFactor = 0.985;
  // v56: goal share ≠ winPct
  const goalShare = 0.5 + (winPct - 0.5) * 0.60;
  const lh = expTotal*goalShare*regFactor, la = expTotal*(1-goalShare)*regFactor;
  let pTied = 0; for (let k=0; k<=15; k++) pTied += poissonPMF(k,lh)*poissonPMF(k,la);
  const calibrated = pTied * 1.28;
  return Math.min(calibrated, 0.40);
}

// v21: Series total goals — returns FULL PMF, not just a mean lambda.
// For each series path (enumerated by win probabilities), compute the Poisson
// convolution of per-game goal distributions (actual score for played games,
// Poisson(expTotal) for unplayed), then mix paths by their probability.
function computeSeriesGoalsPMF(effG, maxK=80) {
  // Cache per-game PMFs so we don't rebuild them every path
  const gamePMFs = effG.map(g => {
    if (g.result && g.homeScore!=null && g.awayScore!=null) {
      // Played game with known score: a degenerate PMF at that exact total
      const total = Number(g.homeScore) + Number(g.awayScore);
      const arr = new Array(maxK+1).fill(0);
      arr[Math.min(total, maxK)] = 1;
      return arr;
    }
    return poissonPMFArray(g.expTotal || 5.5, maxK);
  });
  let acc = new Array(maxK+1).fill(0);
  function rec(gi, hw, aw, prob, pathPMF) {
    if (hw===4 || aw===4 || gi>=7) {
      // Mix this path into accumulator
      for (let k=0; k<=maxK; k++) acc[k] += pathPMF[k] * prob;
      return;
    }
    const g = effG[gi];
    const gp = gamePMFs[gi];
    const nextPMF = convolve(pathPMF, gp, maxK);
    if (g.result==="home") rec(gi+1, hw+1, aw, prob, nextPMF);
    else if (g.result==="away") rec(gi+1, hw, aw+1, prob, nextPMF);
    else {
      rec(gi+1, hw+1, aw, prob*g.winPct, nextPMF);
      rec(gi+1, hw, aw+1, prob*(1-g.winPct), nextPMF);
    }
  }
  // Start with degenerate PMF at 0
  const start = new Array(maxK+1).fill(0); start[0] = 1;
  rec(0,0,0,1,start);
  return acc;
}

// Legacy wrapper that just returns a mean — retained for places that only need lambda (OT series, etc.)
function computeSeriesGoalsLambda(effG) {
  const pmf = computeSeriesGoalsPMF(effG, 80);
  let m = 0; for (let k=0; k<pmf.length; k++) m += k*pmf[k];
  return Math.max(0.01, m);
}

// v21: Shutouts — full PMF, tied to each game's expTotal.
// P(home shutout in game g) = P(homeScore=0) = exp(-expTotal × winPct) under Poisson.
// P(away shutout) = exp(-expTotal × (1-winPct)). These are disjoint (both teams scoring 0 = scoreless tie, impossible in playoff since they go to OT).
// Per-game shutout PMF: {0: 1-pH-pA, 1: pH+pA}. For played games: count actual shutouts.
// Series shutout count PMF = convolution of game PMFs, mixed over paths.
// User's shutoutRate input is now a residual "shutout multiplier" factor applied on top of the model.
function computeShutoutPMF(effG, shutoutRateMultiplier=1.0, maxK=8) {
  const gamePMFs = effG.map(g => {
    if (g.result && g.homeScore!=null && g.awayScore!=null) {
      const n = (Number(g.homeScore)===0 ? 1 : 0) + (Number(g.awayScore)===0 ? 1 : 0);
      const arr = new Array(maxK+1).fill(0);
      arr[Math.min(n, maxK)] = 1;
      return arr;
    }
    const total = g.expTotal || 5.5;
    // v56: same goal-share fix as sim — winPct ≠ goal share
    const goalShare = 0.5 + (g.winPct - 0.5) * 0.60;
    // v92: scale by opposing-goalie quality if known. faceByHome/faceByAway = 1/oppQuality
    // (>1 means goalie is bad → more goals; <1 means elite → fewer goals).
    const fH = g.faceByHome ?? 1.0;
    const fA = g.faceByAway ?? 1.0;
    const lamH = total * goalShare * fH;
    const lamA = total * (1 - goalShare) * fA;
    // v49: P(shutout in game) = P(H=0 ∨ A=0) — inclusion-exclusion.
    // In playoffs both-zero is impossible (OT forced), but the formula is correct as is.
    const pH = Math.exp(-lamH);       // P(home scores 0)
    const pA = Math.exp(-lamA);       // P(away scores 0)
    const pBothZero = pH * pA;        // joint under independence (tiny in playoff range)
    let pOneShutout = (pH + pA - pBothZero) * shutoutRateMultiplier;
    pOneShutout = Math.max(0, Math.min(1, pOneShutout));
    const arr = new Array(maxK+1).fill(0);
    arr[0] = 1 - pOneShutout;
    arr[1] = pOneShutout;
    return arr;
  });
  let acc = new Array(maxK+1).fill(0);
  function rec(gi, hw, aw, prob, pathPMF) {
    if (hw===4 || aw===4 || gi>=7) {
      for (let k=0; k<=maxK; k++) acc[k] += pathPMF[k] * prob;
      return;
    }
    const g = effG[gi];
    const gp = gamePMFs[gi];
    const nextPMF = convolve(pathPMF, gp, maxK);
    if (g.result==="home") rec(gi+1, hw+1, aw, prob, nextPMF);
    else if (g.result==="away") rec(gi+1, hw, aw+1, prob, nextPMF);
    else {
      rec(gi+1, hw+1, aw, prob*g.winPct, nextPMF);
      rec(gi+1, hw, aw+1, prob*(1-g.winPct), nextPMF);
    }
  }
  const start = new Array(maxK+1).fill(0); start[0] = 1;
  rec(0,0,0,1,start);
  return acc;
}
// Legacy: return mean lambda (kept for any consumers that just want the expected count)
function computeShutoutLambda(shutoutRate, expG, effG) {
  // Treat user's shutoutRate input relative to historical baseline 0.08.
  const multiplier = (shutoutRate || 0.08) / 0.08;
  const pmf = computeShutoutPMF(effG || [], multiplier, 8);
  let m = 0; for (let k=0; k<pmf.length; k++) m += k*pmf[k];
  return Math.max(0.0001, m);
}

// OT games in series: enumerate paths, P(k OT games) using Poisson per game
// v87: returns full PMF [p(k=0), p(k=1), ...] for total OT games in series.
// CRITICAL: realized OT games are a deterministic offset, NOT part of the random distribution.
// Earlier code used Poisson(lambda) where lambda included realized — wrong, because Poisson treats
// realized events as random. With realizedOT=3 already in the books, P(final=2) MUST be 0; with the
// old code it was small but non-zero (e.g. 21% adj → +264 American), badly mispricing the market.
//
// The right model: total = realizedOT (fixed) + futureOT (random). Future OT count is a sum of
// independent Bernoulli(pOT) over the unplayed games on each branch — i.e., Poisson-binomial.
// Since pOT≈0.22 is roughly constant per game, we approximate future with Poisson(lambda_future).
// PMF[k] = P(future = k - realizedOT). For k < realizedOT, PMF[k] = 0.
function computeOTSeriesPMF(effG, kMax=10) {
  // 1) Realized OT count from played games
  let realizedOT = 0;
  for (const g of effG) {
    if (g.result && (g.wentOT || g.ot || g.result === "ot")) realizedOT++;
  }
  // 2) Expected FUTURE OT count = E[# unplayed games in series] × avg pOT, computed via tree.
  //    We accumulate (over all branching paths of unplayed games) the path-prob × sum_of_pOT_for_unplayed_games_on_path.
  let lamFuture = 0;
  function rec(gi, hw, aw, prob, futureAcc) {
    if (hw===4||aw===4) { lamFuture += prob * futureAcc; return; }
    if (gi>=7) return;
    const g = effG[gi];
    if (g.result) {
      // Played games contribute nothing to future (they're already in the realized offset)
      if (g.result==="home") rec(gi+1, hw+1, aw, prob, futureAcc);
      else if (g.result==="away") rec(gi+1, hw, aw+1, prob, futureAcc);
    } else {
      const pot = g.pOT ?? 0.22;
      rec(gi+1, hw+1, aw, prob*g.winPct, futureAcc+pot);
      rec(gi+1, hw, aw+1, prob*(1-g.winPct), futureAcc+pot);
    }
  }
  rec(0,0,0,1,0);
  lamFuture = Math.max(0, lamFuture);
  // 3) Future PMF via Poisson approximation, then shift by realizedOT.
  const pmf = new Array(kMax+1).fill(0);
  for (let k=0; k<=kMax; k++) {
    const futureK = k - realizedOT;
    if (futureK < 0) { pmf[k] = 0; continue; }
    pmf[k] = poissonPMF(futureK, lamFuture);
  }
  // Total lambda for display = realized + future
  return { pmf, lambda: realizedOT + lamFuture, realizedOT, lamFuture };
}

function computeOTSeriesDist(effG, outcomes, kMax=8) {
  // Backward-compat shim — kept for any older call sites. New code should use computeOTSeriesPMF.
  const { lambda } = computeOTSeriesPMF(effG, kMax);
  return { lambda };
}

// Spread: home wins - away wins differential.
// Convention (half-lines only): "Home -N.5" means home wins series by at least N+1 games (diff >= N+1).
//                                "Home +N.5" means home either wins or loses by at most N games (diff > -N-1, i.e. diff >= -N).
// v49: drop 0.5 lines (equivalent to series winner market); keep 1.5, 2.5, 3.5 both sides.
function computeSpread(outcomes, homeAbbr, awayAbbr) {
  const rows = [];
  // Home-favoured lines: home -1.5 / -2.5 / -3.5. Home covers iff (hw-aw) > |line| (strictly greater, so -1.5 requires diff >= 2).
  for (const line of [-3.5, -2.5, -1.5]) {
    const absL = Math.abs(line);
    let pHome = 0, pAway = 0;
    for (const [k, prob] of Object.entries(outcomes)) {
      const [hw, aw] = k.split("-").map(Number);
      if ((hw - aw) > absL) pHome += prob; else pAway += prob;
    }
    rows.push({
      homeLabel: `${homeAbbr||"H"} ${line}`,
      awayLabel: `${awayAbbr||"A"} +${absL}`,
      pHome, pAway, line
    });
  }
  // Away-favoured lines: away -1.5 / -2.5 / -3.5. Away covers iff (aw-hw) > |line|.
  for (const line of [-1.5, -2.5, -3.5]) {
    const absL = Math.abs(line);
    let pHome = 0, pAway = 0;
    for (const [k, prob] of Object.entries(outcomes)) {
      const [hw, aw] = k.split("-").map(Number);
      if ((aw - hw) > absL) pAway += prob; else pHome += prob;
    }
    rows.push({
      homeLabel: `${homeAbbr||"H"} +${absL}`,
      awayLabel: `${awayAbbr||"A"} ${line}`,
      pHome, pAway, line: null, awayLine: line
    });
  }
  return rows;
}

// Parlay: NEXT UNPLAYED game winner × series winner (4 combos — both sides of the game × both series outcomes)
// v61 fix: joint probability computed correctly by conditioning series outcome on next-game result.
//           Previous version assumed independence (game × series) which is wrong — game result directly
//           moves the series forward by 1 win.
function computeParlays(effG, outcomes) {
  // Find first game without a result
  let nextIdx = -1;
  for (let i = 0; i < effG.length; i++) {
    if (!effG[i].result) { nextIdx = i; break; }
  }
  if (nextIdx < 0) return { gameNum: null, rows: [] };

  const gwp = effG[nextIdx].winPct;
  const gHome = gwp, gAway = 1 - gwp;

  // Build effG-if-home-wins-G{next} and effG-if-away-wins-G{next}, then re-run outcome recursion
  // to get conditional series winner probabilities.
  const effGifH = effG.map((g,i) => i===nextIdx ? {...g, result:"home"} : g);
  const effGifA = effG.map((g,i) => i===nextIdx ? {...g, result:"away"} : g);
  const oH = computeOutcomes(effGifH);
  const oA = computeOutcomes(effGifA);
  const seriesH_ifH = ["4-0","4-1","4-2","4-3"].reduce((s,k)=>s+(oH[k]||0),0);
  const seriesA_ifH = 1 - seriesH_ifH;
  const seriesH_ifA = ["4-0","4-1","4-2","4-3"].reduce((s,k)=>s+(oA[k]||0),0);
  const seriesA_ifA = 1 - seriesH_ifA;

  return {
    gameNum: nextIdx + 1,
    rows: [
      // P(home wins next game & home wins series) = gHome × P(series home | G won by home)
      { label:`Home wins G${nextIdx+1} & wins series`,  tp: gHome * seriesH_ifH },
      // P(away wins next game & home wins series)
      { label:`Home loses G${nextIdx+1} & wins series`, tp: gAway * seriesH_ifA },
      // P(away wins next game & away wins series)
      { label:`Away wins G${nextIdx+1} & wins series`,  tp: gAway * seriesA_ifA },
      // P(home wins next game & away wins series)
      { label:`Away loses G${nextIdx+1} & wins series`, tp: gHome * seriesA_ifH },
    ]
  };
}

// Team most goals: winner gets (0.5 + shift/2) share of total goals
// P(home most goals) ≈ P(home wins series) * (0.5+shift) + P(away wins) * (0.5-shift) — approx
function computeTeamMostGoals(hwp, awp, shift=0.15) {
  const winnerShare = 0.5 + shift / 2;
  const loserShare = 1 - winnerShare;
  const pHomeMost = hwp * winnerShare + awp * loserShare;
  const pAwayMost = awp * winnerShare + hwp * loserShare;
  const pTied = 1 - pHomeMost - pAwayMost;
  return { pHomeMost, pAwayMost, pTied: Math.max(0, pTied) };
}

// v21: Per-team goals — full PMF. Per-game team lambda = expTotal × (winPct or 1-winPct);
// for played games with scores entered we use the actual team goal as a degenerate PMF.
// NOTE: side="home"/"away" refers to SERIES-HOME/AWAY TEAM, not game host.
function computeTeamGoalsPMF(effG, side, maxK=50) {
  const gamePMFs = effG.map(g => {
    const total = g.expTotal || 5.5;
    const hasActual = g.result && g.homeScore!=null && g.awayScore!=null;
    if (hasActual) {
      const val = side==="home" ? Number(g.homeScore) : Number(g.awayScore);
      const arr = new Array(maxK+1).fill(0);
      arr[Math.min(val, maxK)] = 1;
      return arr;
    }
    // v56: goal share ≠ winPct
    const goalShare = 0.5 + (g.winPct - 0.5) * 0.60;
    // v92: scale by opposing-goalie quality
    const factor = side==="home" ? (g.faceByHome ?? 1.0) : (g.faceByAway ?? 1.0);
    const lam = (side==="home" ? total * goalShare : total * (1 - goalShare)) * factor;
    return poissonPMFArray(Math.max(0.01, lam), maxK);
  });
  let acc = new Array(maxK+1).fill(0);
  function rec(gi, hw, aw, prob, pathPMF) {
    if (hw===4 || aw===4 || gi>=7) {
      for (let k=0; k<=maxK; k++) acc[k] += pathPMF[k] * prob;
      return;
    }
    const g = effG[gi];
    const gp = gamePMFs[gi];
    const nextPMF = convolve(pathPMF, gp, maxK);
    if (g.result==="home") rec(gi+1, hw+1, aw, prob, nextPMF);
    else if (g.result==="away") rec(gi+1, hw, aw+1, prob, nextPMF);
    else {
      rec(gi+1, hw+1, aw, prob*g.winPct, nextPMF);
      rec(gi+1, hw, aw+1, prob*(1-g.winPct), nextPMF);
    }
  }
  const start = new Array(maxK+1).fill(0); start[0] = 1;
  rec(0,0,0,1,start);
  return acc;
}
// Legacy wrapper
function computeTeamGoalsLambda(effG, side) {
  const pmf = computeTeamGoalsPMF(effG, side, 50);
  let m = 0; for (let k=0; k<pmf.length; k++) m += k*pmf[k];
  return Math.max(0.01, m);
}

// O/U table from Poisson CDF
function ouTable(lambda, lines, dispersion=1) {
  return lines.map(line => {
    const lineInt = Math.ceil(line - 0.001);
    const pOver = 1 - nbCDF(lineInt-1, lambda, dispersion);
    return { line, pOver, pUnder: 1-pOver };
  });
}

// ─── DEFAULTS ─────────────────────────────────────────────────────────────────
function defaultSeries(id) {
  return { id, homeTeam:"", awayTeam:"", homeAbbr:"", awayAbbr:"",
    shutoutRate:0.08, winnerGoalShift:0.15,
    games: Array.from({length:7},(_,i)=>({gameNum:i+1,winPct:i===0?0.55:null,expTotal:i===0?5.5:null,pOT:i===0?0.22:null,result:null,homeGoalie:null,awayGoalie:null})) };
}
function defaultMatchup(id) {
  return { id, homeTeam:"", awayTeam:"", homeAbbr:"", awayAbbr:"",
    homeWinPct:0.55, expTotal:5.5, homeWins:0, awayWins:0, expGames:5.82 };
}
// v38: round-aware data structure. Each round holds its own series/matchup arrays.
//   r1: 8 series, r2: 4 series, r3: 2 series, f: 1 series.
// The `bracket` defines how R1 winners pair into R2 (and so on). Default = sequential pairs.
const ROUND_IDS = ["r1", "r2", "r3", "f"];
const ROUND_LABELS = { r1: "R1", r2: "R2", r3: "R3", f: "Final" };
const ROUND_SERIES_COUNT = { r1: 8, r2: 4, r3: 2, f: 1 };
const DEFAULT_BRACKET = {
  // r2pairs[i] = [r1SeriesIdxA, r1SeriesIdxB] — winners of those two series face off in r2 series i
  r2pairs: [[0, 1], [2, 3], [4, 5], [6, 7]],
  r3pairs: [[0, 1], [2, 3]],   // r2 winners → r3 series
  fpair:   [0, 1],             // r3 winners → final
};
function defaultRoundedSeries() {
  const out = {};
  for (const r of ROUND_IDS) out[r] = Array.from({length: ROUND_SERIES_COUNT[r]}, (_, i) => defaultSeries(i));
  return out;
}
function defaultRoundedMatchups() {
  const out = {};
  for (const r of ROUND_IDS) out[r] = Array.from({length: ROUND_SERIES_COUNT[r]}, (_, i) => defaultMatchup(i));
  return out;
}
// v38: migration — old localStorage stored a flat 8-series array under "nhl_s". If we encounter that
// shape, wrap it as r1, fresh defaults for r2/r3/f. Detected by Array.isArray.
function migrateSeries(loaded) {
  if (!loaded) return defaultRoundedSeries();
  if (Array.isArray(loaded)) {
    const fresh = defaultRoundedSeries();
    fresh.r1 = loaded.length === 8 ? loaded : fresh.r1;
    return fresh;
  }
  // Already round-keyed — fill any missing rounds with defaults
  const out = {};
  for (const r of ROUND_IDS) out[r] = (loaded[r] && Array.isArray(loaded[r])) ? loaded[r] : Array.from({length: ROUND_SERIES_COUNT[r]}, (_, i) => defaultSeries(i));
  return out;
}
function migrateMatchups(loaded) {
  if (!loaded) return defaultRoundedMatchups();
  if (Array.isArray(loaded)) {
    const fresh = defaultRoundedMatchups();
    fresh.r1 = loaded.length === 8 ? loaded : fresh.r1;
    return fresh;
  }
  const out = {};
  for (const r of ROUND_IDS) out[r] = (loaded[r] && Array.isArray(loaded[r])) ? loaded[r] : Array.from({length: ROUND_SERIES_COUNT[r]}, (_, i) => defaultMatchup(i));
  return out;
}
const DEFAULT_MARGINS = {
  eightWay:1.12, winner:1.04, length:1.08, spread:1.04,
  totalGoals:1.05, winOrder:1.15, shutouts:1.05, correctScore:1.12,
  parlay:1.08, ouGames:1.05, otGames:1.08, otExact:1.08, otScorer:1.20, hatTrick:1.20,
  teamMostGoals:1.05, teamGoals:1.05,
  propsGoals:1.08, propsAssists:1.05, propsPoints:1.05, propsSOG:1.05,
  propsHits:1.05, propsBlocks:1.05, propsTakeaways:1.05,
  propsGiveaways:1.05,
  seriesLeader:1.5, leaderR1:1.5, leaderFull:1.5,
};
const DEFAULT_GLOBALS = { overroundR1:1.15, overroundFull:1.15, powerFactor:1.20, rateDiscount:0.95, dispersion:1.2, seriesLeaderPF:1.15 };

// ─── PARSER ───────────────────────────────────────────────────────────────────

// v29: HR season-long skaters CSV parser. Handles the Hockey Reference player stats CSV
// (https://www.hockey-reference.com/leagues/NHL_2026_skaters.html → Get table as CSV).
// Key edge cases:
// - Header row may be preceded by comment lines starting with "---" or blank lines
// - Players traded mid-season have an aggregate row (Team="2TM"/"3TM") followed by per-team rows;
//   all share the same Rk number. We DROP the aggregate and KEEP the LAST team row per Rk
//   (HR lists teams chronologically, so last = current team).
// - VGK in HR maps to our internal VEG abbr.
// - TOI is "MM:SS" or "HHHH:SS" → parsed to seconds.
function parseHRSkaters(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim() && !l.startsWith("---"));
  let hi = -1, headers = [];
  for (let i = 0; i < lines.length; i++) {
    const c = lines[i].split(",");
    if (c.includes("Player") && c.includes("Team") && c.includes("GP")) {
      hi = i; headers = c; break;
    }
  }
  if (hi === -1) return {error: "Could not find header row. Expected columns: Rk, Player, Team, GP, ..."};

  const col = (name) => headers.indexOf(name);
  const cm = {
    rk: col("Rk"), name: col("Player"), pos: col("Pos"), team: col("Team"),
    gp: col("GP"), g: col("G"), a: col("A"), pim: col("PIM"),
    sog: col("SOG"), blk: col("BLK"), hit: col("HIT"),
    take: col("TAKE"), give: col("GIVE"), toi: col("TOI"), atoi: col("ATOI"),
  };
  if (cm.name < 0 || cm.team < 0 || cm.gp < 0) {
    return {error: "Required columns missing (Player, Team, GP)"};
  }

  const PLAYOFF_TEAMS_SET = new Set(["ANA","BOS","BUF","CAR","COL","DAL","EDM","LAK","MIN","MTL","OTT","PHI","PIT","TBL","UTA","VEG","VGK"]);

  // Group rows by Rk; for trades (multiple rows per Rk), keep the LAST non-aggregate row.
  const byRk = new Map();
  for (let i = hi+1; i < lines.length; i++) {
    const cells = lines[i].split(",");
    if (cells.length < 5) continue;
    const rk = cells[cm.rk]; const team = (cells[cm.team]||"").trim(); const name = (cells[cm.name]||"").trim();
    if (!rk || !name) continue;
    if (!byRk.has(rk)) byRk.set(rk, []);
    byRk.get(rk).push({cells, team, name});
  }

  const parseToi = s => {
    if (!s) return 0;
    const m = String(s).match(/(\d+):(\d+)/);
    return m ? parseInt(m[1])*60 + parseInt(m[2]) : 0;
  };

  const players = [];
  for (const [, rows] of byRk) {
    const teamRows = rows.length > 1 ? rows.filter(r => !/^\d+TM$/.test(r.team)) : rows;
    if (!teamRows.length) continue;
    const chosen = teamRows[teamRows.length - 1]; // last team = current
    const c = chosen.cells;
    let team = (c[cm.team]||"").trim();
    if (team === "VGK") team = "VEG";
    if (!PLAYOFF_TEAMS_SET.has(team)) continue; // skip non-playoff teams (CGY, NYR, etc.)
    const gp = parseInt(c[cm.gp]) || 1;
    const pos = ((cm.pos>=0 ? c[cm.pos] : "F")||"F").trim();
    // HR uses F/D/G/C/RW/LW; we collapse forwards to F for our position model
    const posSimple = pos === "G" ? "G" : pos === "D" ? "D" : "F";
    const g   = parseInt(c[cm.g]) || 0;
    const a   = parseInt(c[cm.a]) || 0;
    const sog = parseInt(c[cm.sog]) || 0;
    const blk = cm.blk>=0 ? (parseInt(c[cm.blk])||0) : 0;
    const hit = cm.hit>=0 ? (parseInt(c[cm.hit])||0) : 0;
    const tk  = cm.take>=0 ? (parseInt(c[cm.take])||0) : 0;
    const give= cm.give>=0 ? (parseInt(c[cm.give])||0) : 0;
    const pim = cm.pim>=0 ? (parseInt(c[cm.pim])||0) : 0;
    const toi = cm.toi>=0 ? parseToi(c[cm.toi]) : 0;
    const defRole = posSimple === "D" ? "D2" : posSimple === "G" ? "BACKUP" : "MID6";
    players.push({
      name: chosen.name, team, pos: posSimple,
      gp, g, a, pts: g+a, sog, hit, blk, tk, pim, give,
      tsa: 0, // not in HR CSV; left at 0
      onIceF: 0, onIceA: 0, // not in HR CSV; xG-based features won't work for HR-only players
      toi,
      g_pg:   gp>0 ? +(g/gp).toFixed(4)   : 0,
      a_pg:   gp>0 ? +(a/gp).toFixed(4)   : 0,
      pts_pg: gp>0 ? +((g+a)/gp).toFixed(4): 0,
      sog_pg: gp>0 ? +(sog/gp).toFixed(4) : 0,
      hit_pg: gp>0 ? +(hit/gp).toFixed(4) : 0,
      blk_pg: gp>0 ? +(blk/gp).toFixed(4) : 0,
      take_pg:gp>0 ? +(tk/gp).toFixed(4)  : 0,
      pim_pg: gp>0 ? +(pim/gp).toFixed(4) : 0,
      tsa_pg: 0, give_pg: gp>0 ? +(give/gp).toFixed(4) : 0,
      lineRole: defRole,
      pGP:0, pG:0, pA:0, pSOG:0, pHIT:0, pBLK:0, pTK:0, pPIM:0, pTSA:0, pGIVE:0,
      _hrSource: true,
    });
  }
  return {players};
}

function parseHR(text) {
  const lines = text.trim().split("\n");
  let hi = -1, headers = [];
  for (let i=0; i<lines.length; i++) {
    const c = lines[i].split("\t").map(s=>s.trim());
    if (c.some(h=>["Player","Skater","Name"].includes(h))) { hi=i; headers=c; break; }
  }
  if (hi===-1) return {error:"No header found — need Player column"};
  const al = { Player:["Player","Skater","Name"], Team:["Team","Tm"], GP:["GP","GamesPlayed"],
    G:["G","Goals"], A:["A","Assists"], SOG:["SOG","S","Shots"],
    HIT:["HIT","H","Hits"], BLK:["BLK","B","Blocked","BS"],
    TK:["TK","Takeaways","Take","TAKE"], PIM:["PIM","PenMin"],
    GV:["GV","Give","Giveaways","GIVE"] };
  const cm = {};
  for (const [k,alts] of Object.entries(al)) {
    for (const a of alts) { const idx=headers.findIndex(h=>h.toLowerCase()===a.toLowerCase()); if(idx!==-1){cm[k]=idx;break;} }
  }
  if (cm.Player===undefined) return {error:"Could not find Player column"};
  const players = [];
  for (let i=hi+1; i<lines.length; i++) {
    const c = lines[i].split("\t").map(s=>s.trim());
    if (!c[cm.Player]||["Player","Rk"].includes(c[cm.Player])) continue;
    const g=cm.G!==undefined?parseInt(c[cm.G])||0:0, a=cm.A!==undefined?parseInt(c[cm.A])||0:0;
    players.push({ name:c[cm.Player], team:cm.Team!==undefined?(c[cm.Team]||"").toUpperCase():"",
      gp:cm.GP!==undefined?parseInt(c[cm.GP])||0:1, g, a, pts:g+a,
      sog:cm.SOG!==undefined?parseInt(c[cm.SOG])||0:0, hit:cm.HIT!==undefined?parseInt(c[cm.HIT])||0:0,
      blk:cm.BLK!==undefined?parseInt(c[cm.BLK])||0:0,
      tk:cm.TK!==undefined?parseInt(c[cm.TK])||0:0,
      pim:cm.PIM!==undefined?parseInt(c[cm.PIM])||0:0,
      give:cm.GV!==undefined?parseInt(c[cm.GV])||0:0 });
  }
  return {players};
}

// ─── UI PRIMITIVES ────────────────────────────────────────────────────────────
function Toggle({label,checked,onChange}) {
  return (
    <label style={{display:"flex",alignItems:"center",gap:8,cursor:"pointer",fontSize:12,userSelect:"none"}}>
      <span style={{width:30,height:17,borderRadius:9,background:checked?"#3b82f6":"var(--color-border-primary)",
        position:"relative",display:"inline-block",transition:"background 0.15s",flexShrink:0}}>
        <span style={{position:"absolute",top:2,left:checked?15:2,width:13,height:13,borderRadius:"50%",
          background:"white",transition:"left 0.15s",boxShadow:"0 1px 2px rgba(0,0,0,0.3)"}}/>
      </span>
      <span style={{color:"var(--color-text-secondary)"}}>{label}</span>
      <input type="checkbox" checked={checked} onChange={e=>onChange(e.target.checked)} style={{display:"none"}}/>
    </label>
  );
}
function NI({value,onChange,min,max,step=0.01,style={}}) {
  return <input type="number" value={value??""} onChange={e=>onChange(parseFloat(e.target.value)||0)}
    min={min} max={max} step={step} style={{width:68,padding:"3px 6px",fontSize:12,fontFamily:"var(--font-mono)",
      background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
      borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",...style}}/>;
}
function Card({children,style={}}) {
  return <div style={{background:"var(--color-background-primary)",border:"0.5px solid var(--color-border-tertiary)",
    borderRadius:"var(--border-radius-lg)",padding:"1rem 1.25rem",...style}}>{children}</div>;
}
function SH({title,sub}) {
  return <div style={{marginBottom:12}}>
    <h2 style={{margin:0,fontSize:10,fontWeight:500,letterSpacing:"0.1em",textTransform:"uppercase",color:"var(--color-text-secondary)"}}>{title}</h2>
    {sub&&<p style={{margin:"2px 0 0",fontSize:10,color:"var(--color-text-tertiary)"}}>{sub}</p>}
  </div>;
}
function TH({cols}) {
  return <thead><tr style={{borderBottom:"0.5px solid var(--color-border-secondary)"}}>
    {cols.map((c,i)=><th key={i} style={{padding:"5px 8px",textAlign:i===0?"left":"right",
      color:"var(--color-text-secondary)",fontWeight:500,fontSize:10,textTransform:"uppercase"}}>{c}</th>)}
  </tr></thead>;
}
function OR({label,tp,ap,showTrue}) {
  const zero = ap!=null && ap < 0.0001;
  return <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:zero?0.4:1}}>
    <td style={{padding:"5px 8px"}}>{label}</td>
    {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{tp!=null?(tp*100).toFixed(1)+"%":"—"}</td>}
    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{ap!=null?(ap*100).toFixed(1)+"%":"—"}</td>
    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:ap&&ap>=0.5?"#4ade80":"var(--color-text-primary)"}}>{zero?"—":ap!=null?fmt(ap):"—"}</td>
    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{zero?"—":ap!=null?toDec(ap).toFixed(2):"—"}</td>
  </tr>;
}
function Seg({options,value,onChange,accent="#3b82f6"}) {
  return <div style={{display:"flex",borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)"}}>
    {options.map(o=><button key={o.id} onClick={()=>onChange(o.id)} style={{
      padding:"5px 11px",fontSize:11,border:"none",borderRight:"0.5px solid var(--color-border-tertiary)",cursor:"pointer",
      background:value===o.id?accent:"var(--color-background-secondary)",
      color:value===o.id?"white":"var(--color-text-secondary)",whiteSpace:"nowrap"}}>{o.label}</button>)}
  </div>;
}
const SEL = {padding:"4px 8px",fontSize:11,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)"};
function roleColor(r) {
  return {
    TOP6:"#10b981", MID6:"#64748b", BOT6:"#f59e0b",
    ACTIVE:"#0ea5e9", ON_ROSTER:"#0ea5e9",
    D2D:"#fbbf24",
    SCRATCHED:"#ef4444", INACTIVE:"#f87171", IR:"#dc2626", CUT:"#7f1d1d",
    D1:"#3b82f6", D2:"#60a5fa", D3:"#93c5fd",
    STARTER:"#a78bfa", BACKUP:"#7c3aed",
  }[r]||"#64748b";
}
// v91: role taxonomy.
//   Skaters (forwards): TOP6 / MID6 / BOT6 / ACTIVE / D2D / IR / CUT
//   Skaters (defense):  D1 / D2 / D3 / ACTIVE / D2D / IR / CUT
//   Goalies:            STARTER / BACKUP / IR / CUT
// Semantics:
//   TOP6/MID6/BOT6/D1/D2/D3 — projected starter, scoring/physical multipliers apply
//   ACTIVE — on roster but healthy scratch; in pool with very small projection (won't play unless promoted)
//   D2D — day-to-day, miss next game only; future game count reduced by 1 for this player
//   IR — out for this series (per-series tag via roleOverrides). Excluded from this series's props.
//        Stays in player DB so you can un-IR for next series.
//   CUT — not on the active playoff roster. Excluded from ALL props (series + playoff-long).
//        Stays in player DB grayed out, sent to bottom in lists.
const OUT_ROLES = new Set(["IR","CUT"]);  // excluded from props pool
// Legacy aliases — old data may still have these. Treated as IR (out for series) at runtime.
const LEGACY_OUT_ROLES = new Set(["SCRATCHED","INACTIVE"]);
// v91: helpers for "is this player out?". Two flavors:
//   isOutGlobally(p) — CUT (no roster spot) → out of every market
//   isOutForSeries(p, series) — checks the global tag PLUS per-series roleOverrides
//     (e.g., series.roleOverrides[playerKey] === "IR" temporarily IR'd for this series only)
function playerKey(p) { return `${p.name}|${p.team}`; }
function effectiveRole(p, series) {
  // Per-series IR/D2D override beats global; global "CUT" can't be overridden upward.
  const global = canonicalRole(p.lineRole);
  if (global === "CUT") return "CUT";
  const ovr = series && series.roleOverrides && series.roleOverrides[playerKey(p)];
  if (ovr) return canonicalRole(ovr);
  return global;
}
function isOutGlobally(p) { return canonicalRole(p.lineRole) === "CUT"; }
function isOutForSeries(p, series) {
  const r = effectiveRole(p, series);
  return r === "IR" || r === "CUT" || LEGACY_OUT_ROLES.has(r);
}
// v91: a player's projected REMAINING games. D2D players miss the immediate next game,
// so they project for one fewer game than the team's remaining schedule. After the next
// game is recorded (whether they sit or play), the user is expected to update the role tag —
// e.g. D2D → BOT6 if they're back in, or remain D2D if they miss again.
function remainingGamesForPlayer(p, series, expG, roundGP) {
  const baseRemaining = Math.max(0, expG - (roundGP || 0));
  const role = effectiveRole(p, series);
  if (role === "D2D") return Math.max(0, baseRemaining - 1);
  return baseRemaining;
}
function rolesForPos(pos) {
  if(!pos) return ["TOP6","MID6","BOT6","ACTIVE","D2D","IR","CUT"];
  const p=pos.toUpperCase();
  if(p==="G") return ["STARTER","BACKUP","IR","CUT"];
  if(p==="D") return ["D1","D2","D3","ACTIVE","D2D","IR","CUT"];
  return ["TOP6","MID6","BOT6","ACTIVE","D2D","IR","CUT"];
}
// v91: migrate legacy roles → v91 set on first read. Returns the canonical role.
function canonicalRole(r) {
  if (!r) return r;
  if (r === "ON_ROSTER") return "ACTIVE";
  if (r === "SCRATCHED" || r === "INACTIVE") return "IR";
  return r;
}
function roleMultiplier(r, stat) {
  if (!r) return 1.0;
  // v91: canonicalize legacy roles to current set on the fly
  const role = canonicalRole(r);
  const scoringStats = stat === "g" || stat === "a" || stat === "pts" || stat === "sog";
  const physicalStats = stat === "hit" || stat === "blk";
  // Out-of-pool roles: zero contribution
  if (role === "IR" || role === "CUT") return 0;
  // STARTER/BACKUP are goalie-only — exclude from skater pools
  if (role === "STARTER" || role === "BACKUP") return 0;
  if (stat == null) return 1.0;
  if (scoringStats) {
    return {
      TOP6:1.12, MID6:1.00, BOT6:0.90,
      ACTIVE:0.20,  // healthy scratch — token value (won't play unless promoted)
      D2D:1.00,     // when active, plays normal role; D2D handled at game-count level
      D1:1.03, D2:1.00, D3:0.85,
    }[role] ?? 1.0;
  }
  if (physicalStats) {
    return {
      TOP6:0.95, MID6:1.00, BOT6:1.10,
      ACTIVE:0.20, D2D:1.00,
      D1:1.00, D2:1.05, D3:1.08,
    }[role] ?? 1.0;
  }
  return 1.0;
}
function RoleBadge({role}) { const c=roleColor(role||"MID6"); return <span style={{fontSize:9,padding:"1px 5px",borderRadius:3,background:`${c}20`,color:c,fontWeight:500}}>{role||"—"}</span>; }
function SyncBadge({status}) {
  const m={idle:["#6b7280","Offline"],syncing:["#f59e0b","Syncing…"],ok:["#10b981","Synced"],err:["#ef4444","Sync Error"]};
  const [color,label]= m[status]||m.idle;
  return <span style={{fontSize:10,padding:"2px 8px",borderRadius:10,background:`${color}20`,color,fontWeight:500}}>{label}</span>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROOT
// ═══════════════════════════════════════════════════════════════════════════════

// v25: error boundary so a render crash shows a visible error (not a blank page).
class ErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { error: null, info: null }; }
  static getDerivedStateFromError(error) { return { error }; }
  componentDidCatch(error, info) { this.setState({ error, info }); console.error("ErrorBoundary caught:", error, info); }
  render() {
    if (this.state.error) {
      return <div style={{padding:20,fontFamily:"monospace",color:"#ef4444",background:"#1a0a0a",minHeight:"100vh"}}>
        <div style={{fontSize:16,fontWeight:600,marginBottom:10}}>⚠ Render error</div>
        <div style={{fontSize:12,color:"#fca5a5",marginBottom:10}}>{String(this.state.error)}</div>
        <pre style={{fontSize:11,color:"#f87171",background:"#000",padding:10,borderRadius:4,overflow:"auto",maxHeight:300}}>{this.state.error?.stack||""}</pre>
        {this.state.info?.componentStack && <pre style={{fontSize:11,color:"#f87171",background:"#000",padding:10,borderRadius:4,overflow:"auto",maxHeight:300,marginTop:10}}>{this.state.info.componentStack}</pre>}
        <div style={{marginTop:16,display:"flex",gap:8,flexWrap:"wrap"}}>
          <button onClick={()=>this.setState({error:null,info:null})} style={{padding:"6px 12px",fontSize:12,background:"#3b82f6",color:"white",border:"none",borderRadius:4,cursor:"pointer"}}>Retry render</button>
          <button onClick={()=>{try{localStorage.clear();location.reload();}catch(e){}}} style={{padding:"6px 12px",fontSize:12,background:"#ef4444",color:"white",border:"none",borderRadius:4,cursor:"pointer"}}>Clear all local data + reload</button>
        </div>
      </div>;
    }
    return this.props.children;
  }
}

function AppRoot() {
  return <ErrorBoundary><AppInner/></ErrorBoundary>;
}

function AppInner() {
  const [dark,setDark] = useState(true);
  const [tab,setTab] = useState("leaders");
  const [globals,setGlobals] = useState(()=>{try{const s=localStorage.getItem("nhl_globals");return s?{...DEFAULT_GLOBALS,...JSON.parse(s)}:DEFAULT_GLOBALS;}catch{return DEFAULT_GLOBALS;}});
  const [margins,setMargins] = useState(()=>{
    try{
      const s=localStorage.getItem("nhl_margins");
      if(!s) return DEFAULT_MARGINS;
      const parsed = JSON.parse(s);
      // v49: remove deprecated keys
      delete parsed.propsPIM;
      return {...DEFAULT_MARGINS,...parsed};
    }catch{return DEFAULT_MARGINS;}
  });
  const [showTrue,setShowTrue] = useState(false);
  const [showDec,setShowDec] = useState(true);
  const [syncStatus,setSyncStatus] = useState("idle");
  const [players,setPlayers] = useState(()=>{
    try{
      const s=localStorage.getItem("nhl_p");
      if(!s) return null;
      const raw = JSON.parse(s);
      // v20: auto-migrate legacy players (pGP>0 but no pGames array)
      return Array.isArray(raw) ? raw.map(migratePlayer) : raw;
    }catch{return null;}
  });
  const [goalies,setGoalies] = useState(()=>{try{const s=localStorage.getItem("nhl_g");return s?JSON.parse(s):null;}catch{return null;}});
  const [matchups,setMatchups] = useState(()=>{
    try{const s=localStorage.getItem("nhl_m");return migrateMatchups(s?JSON.parse(s):null);}
    catch{return defaultRoundedMatchups();}
  });
  const [advancement,setAdvancement] = useState(()=>{try{const s=localStorage.getItem("nhl_adv");return s?JSON.parse(s):PLAYOFF_TEAMS.reduce((a,t)=>({...a,[t]:{winR1:0.5,winConf:0.25,winCup:0.1}}),{});}catch{return PLAYOFF_TEAMS.reduce((a,t)=>({...a,[t]:{winR1:0.5,winConf:0.25,winCup:0.1}}),{});}});
  const [allSeries,setAllSeries] = useState(()=>{
    try{const s=localStorage.getItem("nhl_s");return migrateSeries(s?JSON.parse(s):null);}
    catch{return defaultRoundedSeries();}
  });
  // v38: bracket structure — defines how series in each round chain to the next.
  const [bracket,setBracket] = useState(()=>{try{const s=localStorage.getItem("nhl_bracket");return s?{...DEFAULT_BRACKET,...JSON.parse(s)}:DEFAULT_BRACKET;}catch{return DEFAULT_BRACKET;}});
  // v38: which round is currently being viewed in the Series Pricer / Leader Markets tabs.
  const [currentRound,setCurrentRound] = useState(()=>{try{return localStorage.getItem("nhl_round")||"r1";}catch{return "r1";}});
  // v31: cross-component signal — every time GameStatImporter commits a game upload, this bumps.
  // SeriesTab watches it and auto-runs the unified sim so prices reflect the new state without a manual click.
  const [gameUploadCounter, setGameUploadCounter] = useState(0);
  const bumpGameUpload = useCallback(()=>setGameUploadCounter(n=>n+1), []);
  // v34: sim results live at App level keyed by series so they SURVIVE tab/series navigation.
  // Only cleared when user explicitly hits Run (which replaces) or when series teams change.
  // Shape: { [seriesKey]: { result, key, ts } }
  const [simResultsBySeries, setSimResultsBySeries] = useState(()=>{
    try{const s=localStorage.getItem("nhl_sim_cache");return s?JSON.parse(s):{};}catch{return {};}
  });
  const setSimForSeries = useCallback((seriesKey, payload)=>{
    setSimResultsBySeries(prev => ({...prev, [seriesKey]: payload}));
  }, []);
  const [lScope,setLScope] = useState("r1");
  const [lStat,setLStat] = useState("g");
  const [lTopN,setLTopN] = useState(25);
  // v57: linemates map for assist correlation. Derived from MoneyPuck lines.csv.
  // Shape: { "normName|team": [{mate: "normName", sharedTOI: seconds}, ...] } — top 3 mates by shared TOI.
  const [linemates,setLinemates] = useState(()=>{
    try{const s=localStorage.getItem("nhl_linemates");return s?JSON.parse(s):{};}catch{return{};}
  });

  useEffect(()=>{document.body.style.background=dark?"#0d0f1a":"#f1f3f7";},[dark]);
  // v79: inject select/option dark-mode styling. The browser-native <option> dropdown panel
  // doesn't inherit page CSS — without color-scheme it renders OS-default which on dark themes
  // produced unreadable white-on-light-grey text. color-scheme:dark tells the browser to use
  // dark dropdown chrome; explicit option color/background kicks in on platforms that ignore it.
  useEffect(()=>{
    let el = document.getElementById("nhl-pricer-globals");
    if (!el) {
      el = document.createElement("style");
      el.id = "nhl-pricer-globals";
      document.head.appendChild(el);
    }
    if (dark) {
      el.textContent = `
        select { color-scheme: dark; }
        select option { background-color: #1a1d2e; color: #e2e8f0; }
        select option:checked { background-color: #2a3050; color: #fff; }
        input { color-scheme: dark; }
      `;
    } else {
      el.textContent = `
        select { color-scheme: light; }
        select option { background-color: #fff; color: #1a202c; }
        select option:checked { background-color: #dbeafe; color: #1a202c; }
        input { color-scheme: light; }
      `;
    }
  },[dark]);
  useEffect(()=>{try{localStorage.setItem("nhl_linemates",JSON.stringify(linemates));}catch{}},[linemates]);
  useEffect(()=>{if(players)localStorage.setItem("nhl_p",JSON.stringify(players));},[players]);
  useEffect(()=>{if(goalies)localStorage.setItem("nhl_g",JSON.stringify(goalies));},[goalies]);
  useEffect(()=>{localStorage.setItem("nhl_m",JSON.stringify(matchups));},[matchups]);
  useEffect(()=>{localStorage.setItem("nhl_adv",JSON.stringify(advancement));},[advancement]);
  useEffect(()=>{localStorage.setItem("nhl_s",JSON.stringify(allSeries));},[allSeries]);
  useEffect(()=>{localStorage.setItem("nhl_bracket",JSON.stringify(bracket));},[bracket]);
  useEffect(()=>{localStorage.setItem("nhl_round",currentRound);},[currentRound]);
  useEffect(()=>{localStorage.setItem("nhl_globals",JSON.stringify(globals));},[globals]);
  useEffect(()=>{localStorage.setItem("nhl_margins",JSON.stringify(margins));},[margins]);
  useEffect(()=>{
    try { localStorage.setItem("nhl_sim_cache", JSON.stringify(simResultsBySeries)); }
    catch (e) { /* may exceed quota; sim cache is recomputable so ignore */ }
  }, [simResultsBySeries]);

  // v52: Explicit Push/Pull model — auto-sync disabled.
  // scheduleSync() is now a no-op. Call doPush() / doPull() from Settings → Cloud Sync.
  // This avoids silent data loss from race conditions and offline sync failures.
  const [lastPushedAt, setLastPushedAt] = useState(() => {
    try { return localStorage.getItem("nhl_last_pushed_at") || null; } catch { return null; }
  });
  const [lastPulledAt, setLastPulledAt] = useState(() => {
    try { return localStorage.getItem("nhl_last_pulled_at") || null; } catch { return null; }
  });
  const [cloudInfo, setCloudInfo] = useState(null);    // {updated_at, device} of the cloud-side latest push we know about
  function scheduleSync(){ /* disabled in v52 — explicit push/pull only */ }
  function setP(v){
    setPlayers(prev => typeof v === "function" ? v(prev) : v);
  }
  function setG(v){
    setGoalies(prev => typeof v === "function" ? v(prev) : v);
  }
  function setM(v){setMatchups(v);}
  function setAdv(v){
    setAdvancement(prev => typeof v === "function" ? v(prev) : v);
  }
  function setSeries(v){
    setAllSeries(prev => typeof v === "function" ? v(prev) : v);
  }
  // v38: per-round adapter — components see a flat array for the active round, but writes
  // route into the round-keyed structure. Backward-compatible with all flat-array consumers.
  const seriesForRound = (allSeries[currentRound] || []);
  const setSeriesForRound = useCallback((v) => {
    setAllSeries(prev => {
      const cur = prev[currentRound] || [];
      const next = typeof v === "function" ? v(cur) : v;
      return {...prev, [currentRound]: next};
    });
  }, [currentRound]);
  const matchupsForRound = (matchups[currentRound] || []);
  const setMatchupsForRound = useCallback((v) => {
    setMatchups(prev => {
      const cur = prev[currentRound] || [];
      const next = typeof v === "function" ? v(cur) : v;
      return {...prev, [currentRound]: next};
    });
  }, [currentRound]);

  // v52: On app load, check cloud for a fresher copy (but don't auto-pull).
  // Shows a "cloud is newer than your local by X min" indicator so user can decide to Pull.
  useEffect(()=>{
    if(!isSbEnabled())return;
    (async()=>{
      // Ping a single representative key to get timestamp of latest cloud push
      const rec = await sbLoad("_meta");
      if (rec && rec.value) setCloudInfo({updated_at: rec.value.updated_at, device: rec.value.device});
    })();
  },[]);

  // v52: Push — snapshot all state to cloud under several keys + a _meta summary
  // v54: Added safety checks to avoid pushing empty/suspicious data + per-key size logging.
  async function doPush(){
    if (!isSbEnabled()) return { ok:false, error: "Cloud Sync not configured" };
    // v54: safety checks
    const warnings = [];
    if (!Array.isArray(players) || players.length < 50) warnings.push(`players array has only ${players?.length||0} entries — looks empty/incomplete`);
    if (!Array.isArray(goalies) || goalies.length < 10) warnings.push(`goalies array has only ${goalies?.length||0} entries`);
    const advKeys = Object.keys(advancement||{});
    if (advKeys.length < 8) warnings.push(`advancement has only ${advKeys.length} teams — expected 16`);
    if (warnings.length > 0) {
      const proceed = window.confirm(`⚠ About to push state that looks incomplete:\n\n${warnings.join("\n")}\n\nContinue pushing anyway? (This will overwrite the cloud copy.)`);
      if (!proceed) return { ok:false, error: "Cancelled by user" };
    }
    setSyncStatus("syncing");
    const cfg = getSbConfig();
    const device = cfg.device || "unknown";
    const now = new Date().toISOString();
    try {
      // v54: log sizes so user can see what's being pushed
      const payload = {
        players, goalies, matchups, advancement,
        series: allSeries, globals, margins, bracket, linemates,
      };
      const sizes = {};
      for (const [k,v] of Object.entries(payload)) {
        try { sizes[k] = JSON.stringify(v).length; } catch { sizes[k] = -1; }
      }
      console.log("[CloudPush] sizes (bytes):", sizes);
      const results = await Promise.all([
        sbSave("players", players, device),
        sbSave("goalies", goalies, device),
        sbSave("matchups", matchups, device),
        sbSave("advancement", advancement, device),
        sbSave("series", allSeries, device),
        sbSave("globals", globals, device),
        sbSave("margins", margins, device),
        sbSave("bracket", bracket, device),
        sbSave("linemates", linemates, device),
        sbSave("_meta", {updated_at: now, device, sizes}, device),
      ]);
      const keyNames = ["players","goalies","matchups","advancement","series","globals","margins","bracket","linemates","_meta"];
      const failedKeys = results.map((ok,i)=>ok?null:keyNames[i]).filter(Boolean);
      const allOk = failedKeys.length === 0;
      if (allOk) {
        setLastPushedAt(now);
        setCloudInfo({updated_at: now, device});
        try { localStorage.setItem("nhl_last_pushed_at", now); } catch {}
        setSyncStatus("ok");
        return { ok:true, at: now, sizes };
      } else {
        setSyncStatus("err");
        return { ok:false, error:`Failed keys: ${failedKeys.join(", ")}` };
      }
    } catch (e) {
      setSyncStatus("err");
      return { ok:false, error: e.message };
    }
  }

  // v54: Verify — fetch all cloud keys and report sizes + sample content (no state overwrite).
  // Lets user see exactly what's in cloud without risking local overwrite.
  async function doVerify(){
    if (!isSbEnabled()) return { ok:false, error: "Cloud Sync not configured" };
    setSyncStatus("syncing");
    try {
      const keys = ["players","goalies","matchups","advancement","series","globals","margins","bracket","linemates","_meta"];
      const recs = await Promise.all(keys.map(k => sbLoad(k)));
      const report = {};
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        const r = recs[i];
        if (!r) { report[k] = "MISSING"; continue; }
        const v = r.value;
        let desc;
        if (Array.isArray(v)) desc = `array len=${v.length}`;
        else if (v && typeof v === "object") desc = `object keys=${Object.keys(v).length} [${Object.keys(v).slice(0,5).join(",")}${Object.keys(v).length>5?"...":""}]`;
        else desc = `${typeof v}`;
        const size = (()=>{try { return JSON.stringify(v).length; } catch { return -1; }})();
        report[k] = `${desc}, ${size} bytes, pushed ${r.updated_at} from ${r.device||"?"}`;
      }
      setSyncStatus("ok");
      return { ok:true, report };
    } catch (e) {
      setSyncStatus("err");
      return { ok:false, error: e.message };
    }
  }

  // v52: Pull — overwrite all local state with cloud copies
  // v55: Post-pull diagnostic that reads BACK localStorage to confirm writes actually landed
  async function doPull(){
    if (!isSbEnabled()) return { ok:false, error: "Cloud Sync not configured" };
    setSyncStatus("syncing");
    try {
      const [pRec,gRec,mRec,aRec,sRec,glRec,mgRec,brRec,lmRec,mtRec] = await Promise.all([
        sbLoad("players"), sbLoad("goalies"), sbLoad("matchups"),
        sbLoad("advancement"), sbLoad("series"),
        sbLoad("globals"), sbLoad("margins"), sbLoad("bracket"),
        sbLoad("linemates"),
        sbLoad("_meta"),
      ]);
      // v54: diagnostic report — which keys populated, sizes
      const diag = {};
      const tag = (k, rec, v) => {
        if (!rec) { diag[k] = "cloud row missing"; return; }
        if (v == null) { diag[k] = "value null"; return; }
        if (Array.isArray(v)) diag[k] = `array len=${v.length}`;
        else if (typeof v === "object") diag[k] = `obj keys=${Object.keys(v).length}`;
        else diag[k] = typeof v;
      };
      tag("players", pRec, pRec?.value);
      tag("goalies", gRec, gRec?.value);
      tag("matchups", mRec, mRec?.value);
      tag("advancement", aRec, aRec?.value);
      tag("series", sRec, sRec?.value);
      tag("globals", glRec, glRec?.value);
      tag("margins", mgRec, mgRec?.value);
      tag("bracket", brRec, brRec?.value);
      tag("linemates", lmRec, lmRec?.value);
      console.log("[CloudPull] payload from cloud:", diag);
      // v55: write state + directly write localStorage (bypass React useEffect timing)
      //      so the next render unambiguously sees the new values.
      if (pRec?.value) {
        setPlayers(pRec.value);
        try { localStorage.setItem("nhl_p", JSON.stringify(pRec.value)); } catch {}
      }
      if (gRec?.value) {
        setGoalies(gRec.value);
        try { localStorage.setItem("nhl_g", JSON.stringify(gRec.value)); } catch {}
      }
      if (mRec?.value) {
        const migrated = migrateMatchups(mRec.value);
        setMatchups(migrated);
        try { localStorage.setItem("nhl_m", JSON.stringify(migrated)); } catch {}
      }
      if (aRec?.value) {
        setAdvancement(aRec.value);
        try { localStorage.setItem("nhl_adv", JSON.stringify(aRec.value)); } catch {}
      }
      if (sRec?.value) {
        const migrated = migrateSeries(sRec.value);
        setAllSeries(migrated);
        try { localStorage.setItem("nhl_s", JSON.stringify(migrated)); } catch {}
      }
      if (glRec?.value) {
        setGlobals(glRec.value);
        try { localStorage.setItem("nhl_globals", JSON.stringify(glRec.value)); } catch {}
      }
      if (mgRec?.value) {
        setMargins(mgRec.value);
        try { localStorage.setItem("nhl_margins", JSON.stringify(mgRec.value)); } catch {}
      }
      if (brRec?.value) {
        setBracket(brRec.value);
        try { localStorage.setItem("nhl_bracket", JSON.stringify(brRec.value)); } catch {}
      }
      if (lmRec?.value) {
        setLinemates(lmRec.value);
        try { localStorage.setItem("nhl_linemates", JSON.stringify(lmRec.value)); } catch {}
      }
      const now = new Date().toISOString();
      setLastPulledAt(now);
      try { localStorage.setItem("nhl_last_pulled_at", now); } catch {}
      if (mtRec?.value) setCloudInfo({updated_at: mtRec.value.updated_at, device: mtRec.value.device});
      setSyncStatus("ok");
      // v55: post-pull read-back — verify what actually landed in localStorage
      const readback = {};
      try {
        const p = JSON.parse(localStorage.getItem("nhl_p") || "[]");
        const a = JSON.parse(localStorage.getItem("nhl_adv") || "{}");
        readback.players = `len=${p.length}`;
        readback.roles = Object.entries(p.reduce((acc,x)=>{const r=x.lineRole||"(empty)";acc[r]=(acc[r]||0)+1;return acc;},{})).map(([k,v])=>`${k}:${v}`).join(" ");
        readback.advancement = Object.keys(a).length + " teams";
        const sample = a.ANA;
        if (sample) readback.ANA = `R1=${sample.winR1?.toFixed(3)} Conf=${sample.winConf?.toFixed(3)} Cup=${sample.winCup?.toFixed(4)} [man: R1=${!!sample.manualR1} C=${!!sample.manualConf} Cup=${!!sample.manualCup}]`;
      } catch {}
      console.log("[CloudPull] localStorage readback:", readback);
      return { ok:true, at: now, cloudAt: mtRec?.value?.updated_at, device: mtRec?.value?.device, diag, readback };
    } catch (e) {
      setSyncStatus("err");
      return { ok:false, error: e.message };
    }
  }

  const teamExpGR1 = useMemo(()=>{
    const m={};
    for(const x of (matchups.r1||[])){if(x.homeAbbr)m[x.homeAbbr]=(m[x.homeAbbr]||0)+x.expGames;if(x.awayAbbr)m[x.awayAbbr]=(m[x.awayAbbr]||0)+x.expGames;}
    return m;
  },[matchups.r1]);

  // v77: same for R2
  const teamExpGR2 = useMemo(()=>{
    const m={};
    // Primary: matchups.r2 (older path, may be empty if user populated allSeries instead)
    for(const x of (matchups.r2||[])){if(x.homeAbbr)m[x.homeAbbr]=(m[x.homeAbbr]||0)+x.expGames;if(x.awayAbbr)m[x.awayAbbr]=(m[x.awayAbbr]||0)+x.expGames;}
    // v101: fallback to allSeries.r2 — compute closed-form expGames from games[0].winPct.
    // R2 wide leader market was returning 0 expTotal for all R2 players because matchups.r2
    // wasn't populated (user uses allSeries.r2 instead). That made every player's λ = 0,
    // so the leader market was just 1/N noise (Couturier as random "favorite").
    for (const sr of (allSeries.r2 || [])) {
      if (!sr || !sr.homeAbbr || !sr.awayAbbr) continue;
      if (m[sr.homeAbbr] != null && m[sr.awayAbbr] != null) continue; // already set from matchups
      const wp = (sr.games && sr.games[0] && sr.games[0].winPct) || 0.55;
      const hw = wp, aw = 1 - wp;
      const p4 = Math.pow(hw,4) + Math.pow(aw,4);
      const p5 = 4 * (Math.pow(hw,4)*aw + Math.pow(aw,4)*hw);
      const p6 = 10 * (Math.pow(hw,4)*aw*aw + Math.pow(aw,4)*hw*hw);
      const p7 = 20 * (Math.pow(hw,4)*aw*aw*aw + Math.pow(aw,4)*hw*hw*hw);
      const tot = p4+p5+p6+p7;
      const expG = tot > 0 ? (4*p4+5*p5+6*p6+7*p7)/tot : 5.82;
      if (m[sr.homeAbbr] == null) m[sr.homeAbbr] = expG;
      if (m[sr.awayAbbr] == null) m[sr.awayAbbr] = expG;
    }
    return m;
  },[matchups.r2, allSeries.r2]);

  // v73: detect teams whose R1 series is decided. Used to zero out future games for
  // eliminated players in leader markets (so a guy whose series ended 4-1 doesn't keep
  // accumulating expected goals as if there were ~3.27 more games to play).
  // Maps team abbr → { over: bool, gamesPlayed: int, won: bool }
  const teamR1Status = useMemo(()=>{
    const map = {};
    for (const s of (allSeries.r1 || [])) {
      if (!s || !s.games || !s.homeAbbr || !s.awayAbbr) continue;
      let hw=0, aw=0;
      for (const g of s.games) {
        if (g.result === "home") hw++;
        else if (g.result === "away") aw++;
      }
      const played = hw + aw;
      const over = (hw >= 4 || aw >= 4);
      map[s.homeAbbr] = { over, gamesPlayed: played, won: hw >= 4 };
      map[s.awayAbbr] = { over, gamesPlayed: played, won: aw >= 4 };
    }
    return map;
  }, [allSeries.r1]);

  // v77: same for R2
  const teamR2Status = useMemo(()=>{
    const map = {};
    for (const s of (allSeries.r2 || [])) {
      if (!s || !s.games || !s.homeAbbr || !s.awayAbbr) continue;
      let hw=0, aw=0;
      for (const g of s.games) {
        if (g.result === "home") hw++;
        else if (g.result === "away") aw++;
      }
      const played = hw + aw;
      const over = (hw >= 4 || aw >= 4);
      map[s.homeAbbr] = { over, gamesPlayed: played, won: hw >= 4 };
      map[s.awayAbbr] = { over, gamesPlayed: played, won: aw >= 4 };
    }
    return map;
  }, [allSeries.r2]);

  // v38: helper to compute one series' (hwp, awp) from sim cache or closed-form.
  // Round-aware: sim cache key now includes round prefix.
  // v39: clinch detection — if a team has 4 wins in realized results, return 1.0/0.0 immediately.
  //      Also use closed-form (which always reflects current realized state) when sim cache is stale.
  function seriesWinProbs(sr, roundId) {
    if (!sr || !sr.homeAbbr || !sr.awayAbbr) return null;
    // Clinch check first — overrides everything
    if (sr.games && sr.games.length) {
      let hw=0, aw=0;
      for (const g of sr.games) {
        if (g.result === "home") hw++;
        else if (g.result === "away") aw++;
      }
      if (hw >= 4) return { hwp: 1, awp: 0 };
      if (aw >= 4) return { hwp: 0, awp: 1 };
    }
    // Try sim cache. Check freshness against current effG: if cache key doesn't match the
    // current games array, the sim is stale and closed-form is more trustworthy.
    const seriesKey = roundId + "|" + sr.homeAbbr + "|" + sr.awayAbbr;
    const cached = (simResultsBySeries||{})[seriesKey] || (roundId === "r1" ? (simResultsBySeries||{})[sr.homeAbbr + "|" + sr.awayAbbr] : null);
    if (cached && cached.result && cached.result.winnerProb && cached.key) {
      // Freshness: cached.key looks like "${effKeyAtSimTime}|${roundId}|${homeAbbr}|${awayAbbr}".
      // We can build a current effKey-equivalent for comparison:
      const currentEffKey = JSON.stringify(sr.games || []) + "|" + seriesKey;
      // If cached key matches what we'd compute now, sim is current; use it.
      // (Loose check — the effKey in SeriesTab includes more state, but for routing winner probs
      // close enough since clinch already handled above.)
      if (cached.key.includes(JSON.stringify(sr.games || []))) {
        return { hwp: cached.result.winnerProb.H, awp: cached.result.winnerProb.A };
      }
    }
    // Closed-form fallback (always reflects realized state)
    if (sr.games && sr.games.length) {
      const games = sr.games.map(g => ({...g, winPct: g.winPct ?? 0.5}));
      try {
        const outcomes = computeOutcomes(games);
        const hwp = ["4-0","4-1","4-2","4-3"].reduce((a,k)=>a+(outcomes[k]||0), 0);
        return { hwp, awp: 1 - hwp };
      } catch { return null; }
    }
    return null;
  }

  // v38: P(team wins R1) — direct from R1 series.
  const autoR1ByTeam = useMemo(()=>{
    const out = {};
    for (const sr of ((allSeries.r1)||[])) {
      const wp = seriesWinProbs(sr, "r1");
      if (wp != null) {
        out[sr.homeAbbr] = wp.hwp;
        out[sr.awayAbbr] = wp.awp;
      }
    }
    return out;
  }, [allSeries.r1, simResultsBySeries]);

  // v38: For each team, compute P(team in R2 series i) and P(team wins R2 | in series i).
  // Then sum: P(team wins R2) = Σ_i P(team in series i) × P(team wins series i | in it).
  // For "in series i": team needs to win their R1 series. Series i in R2 is fed by bracket.r2pairs[i] = [r1A, r1B].
  // P(team is in r2 series i) = P(team wins one of those R1 series).
  // P(team wins r2 series i | in it) = avg over R1 opponents weighted by their R1 win prob.
  // Opponent in r2 = winner of the OTHER r1 series in the pair.
  const autoConfByTeam = useMemo(()=>{
    // First, build R1 winner distributions per series: r1Winners[seriesIdx] = { teamAbbr: prob }
    const r1Winners = (allSeries.r1||[]).map((sr, i) => {
      const wp = seriesWinProbs(sr, "r1");
      if (!wp || !sr.homeAbbr || !sr.awayAbbr) return {};
      return { [sr.homeAbbr]: wp.hwp, [sr.awayAbbr]: wp.awp };
    });
    // Now build R2 series pairings: r2 series i is fed by r1 series indexes bracket.r2pairs[i]
    // For each team, for each r2 series i, P(team is in it) and P(team wins it).
    // P(team in r2 series i) = P(team wins their r1 series IF that series is in the r2 pair) summed over the two source series.
    // P(team wins r2 series i | in it) — needs an estimate. Cleanest pre-matchup:
    //   - If user has set up an r2 series (sr.homeAbbr/awayAbbr filled), use the actual sim/closed-form.
    //   - Otherwise, fallback to xG-strength matchup vs each potential r2 opponent.
    const out = {};
    for (let r2Idx = 0; r2Idx < (bracket.r2pairs||[]).length; r2Idx++) {
      const [r1IdxA, r1IdxB] = bracket.r2pairs[r2Idx];
      const winnersA = r1Winners[r1IdxA] || {};
      const winnersB = r1Winners[r1IdxB] || {};
      const r2Series = (allSeries.r2||[])[r2Idx];
      const r2WP = r2Series ? seriesWinProbs(r2Series, "r2") : null;
      // For each candidate team (winner of A): they would face winner of B.
      for (const [teamA, pA] of Object.entries(winnersA)) {
        for (const [teamB, pB] of Object.entries(winnersB)) {
          const pInR2 = pA * pB; // both must win to face each other
          let pWinThisR2;
          if (r2WP && r2Series.homeAbbr === teamA) pWinThisR2 = r2WP.hwp;
          else if (r2WP && r2Series.awayAbbr === teamA) pWinThisR2 = r2WP.awp;
          else if (r2WP && r2Series.homeAbbr === teamB) pWinThisR2 = r2WP.awp;
          else if (r2WP && r2Series.awayAbbr === teamB) pWinThisR2 = r2WP.hwp;
          else {
            // Fall back to xG-strength matchup
            const sA = computeTeamStrength(players, teamA);
            const sB = computeTeamStrength(players, teamB);
            pWinThisR2 = winProbFromStrength(sA, sB, 0, 1.0);
            if (pWinThisR2 == null) pWinThisR2 = 0.5;
          }
          out[teamA] = (out[teamA] || 0) + pInR2 * pWinThisR2;
          // And the inverse: team B winning vs team A
          const pWinForB = 1 - pWinThisR2;
          out[teamB] = (out[teamB] || 0) + pInR2 * pWinForB;
        }
      }
    }
    return out;
  }, [allSeries.r1, allSeries.r2, simResultsBySeries, bracket, players]);

  // v38: P(team wins Cup) = P(team wins Conf) × P(team wins F | in it).
  // Conf final pairing = bracket.r3pairs / fpair chains. Cleanest approximation:
  //   - For each team: P(in F) = P(wins R1) × P(wins R2 | in R1 winner pool) × P(wins R3 | in R2 winner pool)
  //   - Build via convolution over bracket structure.
  // For now we approximate Conf-winner = winning your conference (both r3 series), and Cup = winning both Conf and Final.
  // Skip fully chained F sim — use xG-strength head-to-head once both conf champs are determined.
  const autoCupByTeam = useMemo(()=>{
    // P(team makes conference final) ≈ P(wins R2 chain) — already in autoConfByTeam IF we interpret it that way.
    // But autoConfByTeam is "P(wins R2)" not "P(wins Conf)". To get Conf, need to chain R2 winners through R3.
    // For each R3 series (= conference final): bracket.r3pairs[i] = [r2IdxA, r2IdxB].
    // P(team wins R3 series i) = sum over (teamX from r2A, teamY from r2B) of P(both there) × P(team beats opponent).
    // But P(team in r2A as winner) is not in autoConfByTeam directly — it's P(wins R2) summed across all R2 series.
    // Need per-R2-series winner probabilities.
    const r1Winners = (allSeries.r1||[]).map((sr) => {
      const wp = seriesWinProbs(sr, "r1");
      if (!wp || !sr.homeAbbr || !sr.awayAbbr) return {};
      return { [sr.homeAbbr]: wp.hwp, [sr.awayAbbr]: wp.awp };
    });
    // r2WinnerByR2Series[i] = { team: prob_wins_r2_series_i }
    const r2WinnerByR2Series = (bracket.r2pairs||[]).map((pair, r2Idx) => {
      const [a, b] = pair;
      const wA = r1Winners[a] || {}, wB = r1Winners[b] || {};
      const r2Series = (allSeries.r2||[])[r2Idx];
      const r2WP = r2Series ? seriesWinProbs(r2Series, "r2") : null;
      const dist = {};
      for (const [tA, pA] of Object.entries(wA)) {
        for (const [tB, pB] of Object.entries(wB)) {
          const pBothThere = pA * pB;
          let pAWins;
          if (r2WP && r2Series.homeAbbr === tA) pAWins = r2WP.hwp;
          else if (r2WP && r2Series.awayAbbr === tA) pAWins = r2WP.awp;
          else {
            const sA = computeTeamStrength(players, tA);
            const sB = computeTeamStrength(players, tB);
            pAWins = winProbFromStrength(sA, sB, 0, 1.0);
            if (pAWins == null) pAWins = 0.5;
          }
          dist[tA] = (dist[tA] || 0) + pBothThere * pAWins;
          dist[tB] = (dist[tB] || 0) + pBothThere * (1 - pAWins);
        }
      }
      return dist;
    });
    // r3WinnerByR3Series[i] = { team: prob } — conference champ
    const r3WinnerByR3Series = (bracket.r3pairs||[]).map((pair, r3Idx) => {
      const [a, b] = pair;
      const wA = r2WinnerByR2Series[a] || {}, wB = r2WinnerByR2Series[b] || {};
      const r3Series = (allSeries.r3||[])[r3Idx];
      const r3WP = r3Series ? seriesWinProbs(r3Series, "r3") : null;
      const dist = {};
      for (const [tA, pA] of Object.entries(wA)) {
        for (const [tB, pB] of Object.entries(wB)) {
          const pBothThere = pA * pB;
          let pAWins;
          if (r3WP && r3Series.homeAbbr === tA) pAWins = r3WP.hwp;
          else if (r3WP && r3Series.awayAbbr === tA) pAWins = r3WP.awp;
          else {
            const sA = computeTeamStrength(players, tA);
            const sB = computeTeamStrength(players, tB);
            pAWins = winProbFromStrength(sA, sB, 0, 1.0);
            if (pAWins == null) pAWins = 0.5;
          }
          dist[tA] = (dist[tA] || 0) + pBothThere * pAWins;
          dist[tB] = (dist[tB] || 0) + pBothThere * (1 - pAWins);
        }
      }
      return dist;
    });
    // Final (Cup): bracket.fpair = [r3IdxA, r3IdxB]
    const cupDist = {};
    if (bracket.fpair && bracket.fpair.length === 2) {
      const [a, b] = bracket.fpair;
      const wA = r3WinnerByR3Series[a] || {}, wB = r3WinnerByR3Series[b] || {};
      const fSeries = (allSeries.f||[])[0];
      const fWP = fSeries ? seriesWinProbs(fSeries, "f") : null;
      for (const [tA, pA] of Object.entries(wA)) {
        for (const [tB, pB] of Object.entries(wB)) {
          const pBothThere = pA * pB;
          let pAWins;
          if (fWP && fSeries.homeAbbr === tA) pAWins = fWP.hwp;
          else if (fWP && fSeries.awayAbbr === tA) pAWins = fWP.awp;
          else {
            const sA = computeTeamStrength(players, tA);
            const sB = computeTeamStrength(players, tB);
            pAWins = winProbFromStrength(sA, sB, 0, 1.0);
            if (pAWins == null) pAWins = 0.5;
          }
          cupDist[tA] = (cupDist[tA] || 0) + pBothThere * pAWins;
          cupDist[tB] = (cupDist[tB] || 0) + pBothThere * (1 - pAWins);
        }
      }
    }
    // Conference winners = sum over r3 series (a team appears in only one)
    const confDist = {};
    for (const dist of r3WinnerByR3Series) {
      for (const [t, p] of Object.entries(dist)) confDist[t] = (confDist[t] || 0) + p;
    }
    // v46: P(team wins R2) = P(makes R3) — needed for correct expected-games calc.
    const r2Dist = {};
    for (const dist of r2WinnerByR2Series) {
      for (const [t, p] of Object.entries(dist)) r2Dist[t] = (r2Dist[t] || 0) + p;
    }
    return { conf: confDist, cup: cupDist, r2: r2Dist };
  }, [allSeries, simResultsBySeries, bracket, players]);

  const autoConfByTeamFinal = autoCupByTeam.conf;
  const autoCupByTeamFinal = autoCupByTeam.cup;
  const autoR2ByTeam = autoCupByTeam.r2;

  const computeLambda = useCallback((p,stat,scope)=>{
    const rm=roleMultiplier(p.lineRole, stat);
    if(rm===0)return {actual:0,futureLam:0.0001,lam:0.0001};
    // stat key mapping: tsa->tsa_pg, give->give_pg, tk->take_pg, else stat_pg
    const pgKey=stat==="tk"?"take_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
    // v13: shrink rate with Bayesian prior for <20 GP (shrinkRate handles threshold internally)
    const shrunk = shrinkRate(p[pgKey], p.gp, stat);
    // v66: blend with playoff per-game rate (scoring stats only)
    // v99: scope-aware blend — was using cumulative pG/pGP, now filters by round
    const blended = blendedRate(p, stat, shrunk, scope);
    // v21: stat-category rate adjustment (scoring = raw discount; physical stats go up; neutral stats mild up)
    const rr = blended * rm * globals.rateDiscount * statRateMultiplier(stat);
    let expTotal,actualGP;
    const r1status = teamR1Status[p.team];
    const r2status = teamR2Status[p.team];
    if(scope==="r1"){
      // v73: if R1 series is over for this team, lock expTotal to actual games played.
      // Otherwise use the closed-form expected length.
      if (r1status && r1status.over) {
        expTotal = r1status.gamesPlayed;
      } else {
        expTotal = teamExpGR1[p.team]||5.82;
      }
      // v76: round-filtered GP — pGP accumulates across rounds, but R1 scope should
      // only count R1 games for "remaining games" calc.
      actualGP=readActualGP(p, "r1");
    }
    else if(scope==="r2"){
      // v77: R2 scope mirrors R1. If team's R2 series is decided, lock expTotal to gamesPlayed.
      // If team isn't in R2 (eliminated in R1 or hasn't been seeded yet), expTotal=0 → no future production.
      if (r2status && r2status.over) {
        expTotal = r2status.gamesPlayed;
      } else if (teamExpGR2[p.team] != null) {
        expTotal = teamExpGR2[p.team];
      } else {
        // Team not in R2 — no future games in this scope.
        expTotal = readActualGP(p, "r2");
      }
      actualGP = readActualGP(p, "r2");
    }
    else{
      const adv=advancement[p.team]||{winR1:0.5,winConf:0.25,winCup:0.1,manualR1:false,manualConf:false,manualCup:false};
      // v40: respect per-field manual override flag — if set, use stored value; else use auto.
      const r1 = (autoR1ByTeam[p.team] != null && !adv.manualR1) ? autoR1ByTeam[p.team] : adv.winR1;
      const conf = ((autoConfByTeamFinal||{})[p.team] != null && !adv.manualConf) ? autoConfByTeamFinal[p.team] : adv.winConf;
      const cup = ((autoCupByTeamFinal||{})[p.team] != null && !adv.manualCup) ? autoCupByTeamFinal[p.team] : adv.winCup;
      // v46: E[games] = GPR × (P(plays R1) + P(plays R2) + P(plays R3) + P(plays F))
      //              = GPR × (1 + P(wins R1) + P(wins R2) + P(wins R3))
      // P(wins R1) = r1; P(wins R3) = conf (= P(makes F)); P(wins Cup) = cup.
      // P(wins R2) = P(makes R3): use autoR2ByTeam if available, else interpolate between r1 and conf.
      const autoR2 = (autoR2ByTeam||{})[p.team];
      const r2 = (autoR2 != null && !adv.manualConf) ? autoR2 : Math.sqrt(Math.max(0, r1 * conf));
      // v73: if R1 series is over and team lost, expTotal locks to gamesPlayed (no future).
      // If they won, advancement probs handle further-round expectation correctly.
      if (r1status && r1status.over && !r1status.won) {
        expTotal = r1status.gamesPlayed;
      } else {
        expTotal=5.82*(1 + r1 + r2 + conf);
      }
      // Full playoff scope: use cumulative pGP.
      actualGP=p.pGP||0;
    }
    // v76/77: actual stat for R1 scope is R1-only; R2 is R2-only; full scope is cumulative.
    const actual = readActual(p, stat, scope==="r1" ? "r1" : scope==="r2" ? "r2" : "full");
    const futureLam = Math.max(0.0001, rr * Math.max(0, expTotal - actualGP));
    const lam = Math.max(0.0001, actual + futureLam);
    return {actual, futureLam, lam};
  },[globals.rateDiscount,teamExpGR1,teamExpGR2,teamR1Status,teamR2Status,advancement,autoR1ByTeam,autoConfByTeamFinal,autoCupByTeamFinal,autoR2ByTeam]);

  // v24 Phase E Pt3: Per-matchup series sims. Cached by matchups + globals + players.
  // Stat-independent — running all 9 stats per sim gives us PMFs for every leader market.
  const r1MatchupSims = useMemo(()=>{
    if (tab !== "leaders") return null;
    if (lScope !== "r1") return null;
    if (!players || !players.length) return null;
    const activeMatchups = (matchups.r1 || []).filter(m => m.homeAbbr && m.awayAbbr);
    if (!activeMatchups.length) return null;
    const r = globals.dispersion;
    const HOME_IS_MATCHUP_HOME = [true,true,true,false,false,true,false,true];
    const pGamePlayedCache = [null,1,1,1,1,1,1,1];
    return activeMatchups.map((m, mi) => {
      const wp = m.homeWinPct ?? 0.55;
      const tot = m.expTotal ?? 5.5;
      const effG = [];
      const hw = m.homeWins || 0;
      const aw = m.awayWins || 0;
      let homeWinsRemaining = hw, awayWinsRemaining = aw;
      for (let gi = 0; gi < 7; gi++) {
        const isHomeGame = HOME_IS_MATCHUP_HOME[gi+1];
        const gameWinPct = isHomeGame ? wp : (1 - wp);
        let result = null, homeScore = null, awayScore = null, wentOT = false;
        if (homeWinsRemaining > 0) { result = "home"; homeScore = 3; awayScore = 2; homeWinsRemaining--; }
        else if (awayWinsRemaining > 0) { result = "away"; homeScore = 2; awayScore = 3; awayWinsRemaining--; }
        effG.push({gameNum:gi+1, winPct: gameWinPct, expTotal: tot, pOT: 0.22, result, homeScore, awayScore, wentOT, homeGoalie:null, awayGoalie:null});
      }
      const goalieQualityFaced = effG.map(()=>({faceByHome:1.0, faceByAway:1.0}));
      // v98: this market is R1-only (line 3506 guard); pass "r1" so realized seed is R1 stats only
      const inputs = buildSimInputs(effG, m.homeAbbr, m.awayAbbr, players, globals, goalieQualityFaced, pGamePlayedCache, linemates, "r1");
      if (!inputs.pool.length) return null;
      return simulateSeries(inputs, r, 5000, 70001 + mi);
    }).filter(x => x);
  }, [tab, lScope, players, matchups, globals]);

  // Aggregate across matchups for the currently-selected stat. Fast (5-200ms).
  const r1LeaderMarket = useMemo(()=>{
    if (!r1MatchupSims || !r1MatchupSims.length) return null;
    const or = globals.overroundR1;
    const pf = globals.powerFactor;
    const flat = [];
    for (const sim of r1MatchupSims) {
      for (let i = 0; i < sim.pool.length; i++) {
        const p = sim.pool[i];
        let pmf;
        if (lStat === "pts") {
          const g = sim.playerPMF[i].g;
          const a = sim.playerPMF[i].a;
          const maxK = g.length + a.length - 2;
          pmf = convolve(g, a, maxK);
        } else {
          pmf = sim.playerPMF[i][lStat];
        }
        if (!pmf) continue;
        let mean = 0; for (let k = 0; k < pmf.length; k++) mean += k * pmf[k];
        flat.push({name: p.name, team: p.team, pmf, lambda: mean});
      }
    }
    const raw = flat.length ? simulateLeaderFromPMFs(flat.map(f=>f.pmf), 20000, 88001) : [];
    const adj = applyLeaderOverround(raw, pf, or);
    const pMap = new Map(players.map(p => [p.name+"|"+p.team, p]));
    return flat.map((f,i) => {
      const meta = pMap.get(f.name+"|"+f.team) || {};
      return {...meta, name: f.name, team: f.team, lambda: f.lambda, futureLam: 0, actualStat: 0, trueProb: raw[i], adjProb: adj[i]};
    }).sort((a,b)=>b.adjProb-a.adjProb).slice(0, lTopN);
  }, [r1MatchupSims, lStat, lTopN, globals.overroundR1, globals.powerFactor, players]);

  const leaderMarket = useMemo(()=>{
    if(!players||!players.length)return[];
    // v24 Phase E Pt3: prefer unified R1 market when in R1 scope and it's computable
    if (lScope === "r1" && r1LeaderMarket) return r1LeaderMarket;
    const or=lScope==="r1"?globals.overroundR1:lScope==="r2"?globals.overroundR1:globals.overroundFull;
    const pf=globals.powerFactor;
    const r = globals.dispersion;
    let pool=players.filter(p=>roleMultiplier(p.lineRole)>0);
    if(lScope==="r1"){const r1m=matchups.r1||[];const active=new Set([...r1m.filter(m=>m.homeAbbr).map(m=>m.homeAbbr),...r1m.filter(m=>m.awayAbbr).map(m=>m.awayAbbr)]);if(active.size>0)pool=pool.filter(p=>active.has(p.team));}
    // v77: R2 scope — filter to teams that are in R2 (could be from matchups.r2 or allSeries.r2).
    if(lScope==="r2"){
      const active = new Set();
      for (const m of (matchups.r2||[])) {
        if (m.homeAbbr) active.add(m.homeAbbr);
        if (m.awayAbbr) active.add(m.awayAbbr);
      }
      for (const sr of (allSeries.r2||[])) {
        if (sr.homeAbbr) active.add(sr.homeAbbr);
        if (sr.awayAbbr) active.add(sr.awayAbbr);
      }
      if (active.size>0) pool = pool.filter(p=>active.has(p.team));
    }
    // v43: Full Playoff scope must also filter to playoff teams. Bug: previously included all players,
    // so non-playoff teams (MTL, etc.) got default 50/25/10 advancement → ~10 expected playoff games each →
    // Caufield-style false favourites. Build active set from any series in any round of allSeries.
    if(lScope==="full"){
      const active = new Set();
      for (const r of ROUND_IDS) {
        for (const sr of (allSeries[r]||[])) {
          if (sr.homeAbbr) active.add(sr.homeAbbr);
          if (sr.awayAbbr) active.add(sr.awayAbbr);
        }
      }
      // Also fold in matchups.r1 in case user populated those instead of allSeries.r1
      for (const m of (matchups.r1||[])) {
        if (m.homeAbbr) active.add(m.homeAbbr);
        if (m.awayAbbr) active.add(m.awayAbbr);
      }
      if (active.size > 0) pool = pool.filter(p=>active.has(p.team));
    }
    // v24: Monte Carlo leader probabilities. Handles ties correctly + uses NB dispersion consistent with props.
    const computed = pool.map(p=>computeLambda(p,lStat,lScope));
    const entries = computed.map(c=>({futureLam:c.futureLam, actual:c.actual}));
    const raw = entries.length ? simulateLeader(entries, r, 10000, 12345) : [];
    const adj = applyLeaderOverround(raw, pf, or);
    return pool.map((p,i)=>({...p,lambda:computed[i].lam,futureLam:computed[i].futureLam,actualStat:computed[i].actual,trueProb:raw[i],adjProb:adj[i]})).sort((a,b)=>b.adjProb-a.adjProb).slice(0,lTopN);
  },[players,lStat,lScope,globals,computeLambda,matchups,advancement,lTopN,r1LeaderMarket,allSeries]);

  function exportState(){const s={players,goalies,matchups,allSeries,advancement,globals,margins,bracket};return JSON.stringify(s,null,2);}
  function importState(text){try{const s=JSON.parse(text);if(s.players){setPlayers(s.players);scheduleSync("players",s.players);}if(s.goalies){setGoalies(s.goalies);scheduleSync("goalies",s.goalies);}if(s.matchups){setMatchups(migrateMatchups(s.matchups));}if(s.allSeries){setAllSeries(migrateSeries(s.allSeries));}if(s.advancement){setAdvancement(s.advancement);}if(s.globals)setGlobals(s.globals);if(s.margins)setMargins(s.margins);if(s.bracket)setBracket(s.bracket);return{ok:true};}catch(e){return{ok:false,error:e.message};}}

  const STATS=[
    {id:"g",label:"Goals"},{id:"a",label:"Assists"},{id:"pts",label:"Points"},
    {id:"sog",label:"SOG"},{id:"hit",label:"Hits"},{id:"blk",label:"Blocks"},
    {id:"tk",label:"TK"},{id:"give",label:"GV"},
  ];
  const NAV=[{id:"leaders",l:"Leader Markets"},{id:"series",l:"Series Pricer"},{id:"parlay",l:"Series Parlay Pricer"},{id:"compare",l:"Line Compare"},{id:"upload",l:"Upload Stats"},{id:"stats",l:"Player Stats"},{id:"roles",l:"Roles"},{id:"settings",l:"Settings"}];
  const [gameModal,setGameModal] = useState(null);

  return (
    <div style={{minHeight:"100vh",fontFamily:"var(--font-sans)",background:dark?"#0d0f1a":"#f1f3f7",color:dark?"#e2e8f0":"#1a202c"}}>
      <div style={{position:"sticky",top:0,zIndex:100,background:dark?"#131625":"#fff",
        borderBottom:`0.5px solid ${dark?"#1e2235":"#e2e8f0"}`,display:"flex",alignItems:"center",padding:"0 20px",height:50}}>
        <span style={{fontWeight:500,fontSize:13,letterSpacing:"0.1em",marginRight:24,color:dark?"#60a5fa":"#1d4ed8",flexShrink:0}}>NHL PRICER</span>
        {NAV.map(t=><button key={t.id} onClick={()=>setTab(t.id)} style={{
          padding:"0 14px",height:50,fontSize:12,fontWeight:tab===t.id?500:400,background:"transparent",border:"none",cursor:"pointer",
          borderBottom:tab===t.id?"2px solid #3b82f6":"2px solid transparent",
          color:tab===t.id?(dark?"#93c5fd":"#2563eb"):(dark?"#64748b":"#6b7280"),whiteSpace:"nowrap"}}>{t.l}</button>)}
        <div style={{marginLeft:"auto",display:"flex",gap:12,alignItems:"center",flexShrink:0}}>
          <button onClick={()=>setGameModal({seriesIdx:0,gameIdx:0})} style={{
            padding:"5px 12px",fontSize:11,borderRadius:"var(--border-radius-md)",cursor:"pointer",fontWeight:500,
            background:"#10b981",color:"white",border:"none",letterSpacing:"0.02em"}}>+ Enter Game</button>
          <SyncBadge status={syncStatus}/>
          {players&&<span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>{players.length}p</span>}
          <Toggle label="Light" checked={!dark} onChange={v=>setDark(!v)}/>
        </div>
      </div>

      <div style={{maxWidth:1440,margin:"0 auto",padding:"20px"}}>
        {tab==="leaders"&&<LeadersTab players={players} setPlayers={setP} matchups={matchupsForRound} setMatchups={setMatchupsForRound}
          advancement={advancement} setAdvancement={setAdv} globals={globals} setGlobals={setGlobals}
          leaderMarket={leaderMarket} STATS={STATS} lStat={lStat} setLStat={setLStat}
          lScope={lScope} setLScope={setLScope} lTopN={lTopN} setLTopN={setLTopN}
          showTrue={showTrue} setShowTrue={setShowTrue} showDec={showDec} dark={dark}
          allSeries={seriesForRound} allSeriesByRound={allSeries} simResultsBySeries={simResultsBySeries}
          autoR1ByTeam={autoR1ByTeam} autoConfByTeam={autoConfByTeamFinal} autoCupByTeam={autoCupByTeamFinal}
          currentRound={currentRound} setCurrentRound={setCurrentRound}
          linemates={linemates}/>}
        {tab==="series"&&<SeriesTab allSeries={seriesForRound} setAllSeries={setSeriesForRound}
          players={players} goalies={goalies} margins={margins} setMargins={setMargins}
          linemates={linemates}
          globals={globals} showTrue={showTrue} dark={dark} onEnterGame={setGameModal}
          gameUploadCounter={gameUploadCounter}
          simResultsBySeries={simResultsBySeries} setSimForSeries={setSimForSeries}
          currentRound={currentRound} setCurrentRound={setCurrentRound}/>}
        {tab==="parlay"&&<ParlayTab allSeries={seriesForRound} currentRound={currentRound} margins={margins} dark={dark}/>}
        {tab==="upload"&&<UploadTab players={players} setPlayers={setP} goalies={goalies} setGoalies={setG}
          linemates={linemates} setLinemates={setLinemates}
          exportState={exportState} importState={importState} syncStatus={syncStatus}
          allSeries={seriesForRound} setAllSeries={setSeriesForRound} dark={dark}
          onGameUploaded={bumpGameUpload} currentRound={currentRound}/>}
        {tab==="compare"&&<CompareTab leaderMarket={leaderMarket} STATS={STATS} lStat={lStat} setLStat={setLStat} lScope={lScope} setLScope={setLScope} dark={dark}/>}
        {tab==="stats"&&<PlayerStatsTab players={players} setPlayers={setP} dark={dark}/>}
        {tab==="roles"&&<RolesTab players={players} setPlayers={setP} dark={dark}/>}
        {tab==="settings"&&<SettingsTab globals={globals} setGlobals={setGlobals}
          margins={margins} setMargins={setMargins}
          showTrue={showTrue} setShowTrue={setShowTrue} showDec={showDec} setShowDec={setShowDec}
          doPush={doPush} doPull={doPull} doVerify={doVerify}
          lastPushedAt={lastPushedAt} lastPulledAt={lastPulledAt}
          cloudInfo={cloudInfo} syncStatus={syncStatus}
          dark={dark}/>}
      </div>

      {gameModal!==null&&<GameEntryModal
        dark={dark}
        allSeries={seriesForRound}
        players={players}
        goalies={goalies}
        initialSeriesIdx={gameModal.seriesIdx}
        initialGameIdx={gameModal.gameIdx}
        onClose={()=>setGameModal(null)}
        onCommit={(seriesIdx,gameIdx,result,homeScore,awayScore,playerDeltas,goalieDeltas,wentOT,otScorer)=>{
          // 1. Update series game result (round-aware) — v89: also persist wentOT and otScorer
          setSeriesForRound(prev=>{
            const u=[...prev];
            const games=[...u[seriesIdx].games];
            games[gameIdx]={...games[gameIdx],result,homeScore,awayScore,wentOT:!!wentOT,otScorer:otScorer||null};
            u[seriesIdx]={...u[seriesIdx],games};
            return u;
          });
          // 2. Update player cumulative playoff stats
          if(playerDeltas.length&&players){
            setP(prev=>prev.map(p=>{
              const d=playerDeltas.find(x=>x.name===p.name&&x.team===p.team);
              if(!d)return p;
              return{...p,
                pGP:(p.pGP||0)+1,
                pG:(p.pG||0)+(d.g||0),
                pA:(p.pA||0)+(d.a||0),
                pSOG:(p.pSOG||0)+(d.sog||0),
                pHIT:(p.pHIT||0)+(d.hit||0),
                pBLK:(p.pBLK||0)+(d.blk||0),
              };
            }));
          }
          // 3. Update goalie cumulative saves
          if(goalieDeltas.length&&goalies){
            setG(prev=>prev.map(g=>{
              const d=goalieDeltas.find(x=>x.name===g.name&&x.team===g.team);
              if(!d)return g;
              return{...g,pGP:(g.pGP||0)+1,pSaves:(g.pSaves||0)+(d.saves||0)};
            }));
          }
          setGameModal(null);
        }}
      />}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// GAME ENTRY MODAL
// ═══════════════════════════════════════════════════════════════════════════════
function GameEntryModal({dark,allSeries,players,goalies,initialSeriesIdx,initialGameIdx,onClose,onCommit}) {
  const [seriesIdx,setSeriesIdx] = useState(initialSeriesIdx);
  const [gameIdx,setGameIdx] = useState(initialGameIdx);
  const [winner,setWinner] = useState(""); // "home"|"away"
  const [ot,setOt] = useState(false);
  const [otScorer,setOtScorer] = useState(""); // v89: player name who scored the OT goal
  const [homeScore,setHomeScore] = useState("");
  const [awayScore,setAwayScore] = useState("");
  // playerDeltas: { name, team, g, a, sog, hit, blk }
  const [playerDeltas,setPlayerDeltas] = useState({});
  // goalieDeltas: { name, team, saves }
  const [goalieDeltas,setGoalieDeltas] = useState({});
  const [step,setStep] = useState("game"); // "game" | "skaters" | "goalies" | "review"
  const [filterTeam,setFilterTeam] = useState("all"); // "all"|homeAbbr|awayAbbr
  const [search,setSearch] = useState("");

  const s = allSeries[seriesIdx];
  const gameNum = gameIdx + 1;
  const homeAbbr = s?.homeAbbr||"";
  const awayAbbr = s?.awayAbbr||"";
  const homeName = s?.homeTeam||homeAbbr||"Home";
  const awayName = s?.awayTeam||awayAbbr||"Away";

  // Pool: skaters from both series teams, not scratched
  const pool = useMemo(()=>{
    if(!players||!homeAbbr||!awayAbbr) return [];
    const teams = new Set([homeAbbr,awayAbbr]);
    return players.filter(p=>teams.has(p.team)&&!isOutForSeries(p, s))
      .sort((a,b)=>a.team.localeCompare(b.team)||b.pts-a.pts);
  },[players,homeAbbr,awayAbbr,s]);

  // Goalie pool: both teams, starter share >= 5%
  const goaliePool = useMemo(()=>{
    if(!goalies||!homeAbbr||!awayAbbr) return [];
    const teams = new Set([homeAbbr,awayAbbr]);
    return goalies.filter(g=>teams.has(g.team)&&g.starter_share>=0.05)
      .sort((a,b)=>a.team.localeCompare(b.team)||b.starter_share-a.starter_share);
  },[goalies,homeAbbr,awayAbbr]);

  const displayedPlayers = pool.filter(p=>
    (filterTeam==="all"||p.team===filterTeam)&&
    (!search||p.name.toLowerCase().includes(search.toLowerCase()))
  );

  function setDelta(name,team,field,val){
    const key=`${name}__${team}`;
    setPlayerDeltas(prev=>({...prev,[key]:{...(prev[key]||{name,team}),[field]:Math.max(0,parseInt(val)||0)}}));
  }
  function getDelta(name,team,field){
    return playerDeltas[`${name}__${team}`]?.[field]??0;
  }
  function setGDelta(name,team,field,val){
    const key=`${name}__${team}`;
    setGoalieDeltas(prev=>({...prev,[key]:{...(prev[key]||{name,team}),[field]:Math.max(0,parseInt(val)||0)}}));
  }
  function getGDelta(name,team,field){
    return goalieDeltas[`${name}__${team}`]?.[field]??0;
  }

  // Quick auto-fill pts from g+a
  function autoFillPts(name,team){
    const key=`${name}__${team}`;
    const d=playerDeltas[key]||{};
    setPlayerDeltas(prev=>({...prev,[key]:{...d,name,team}}));
  }

  function handleCommit(){
    const pDeltas=Object.values(playerDeltas).filter(d=>d.g||d.a||d.sog||d.hit||d.blk);
    const gDeltas=Object.values(goalieDeltas).filter(d=>d.saves);
    onCommit(seriesIdx,gameIdx,winner,homeScore,awayScore,pDeltas,gDeltas,ot,ot?otScorer:null);
  }

  const canCommit = winner && (homeScore!==""||awayScore!=="");
  const totalGoals = Object.values(playerDeltas).reduce((s,d)=>s+(d.g||0),0);
  const totalSaves = Object.values(goalieDeltas).reduce((s,d)=>s+(d.saves||0),0);

  const STEPS=[{id:"game",l:"Game Result"},{id:"skaters",l:"Skater Stats"},{id:"goalies",l:"Goalie Saves"},{id:"review",l:"Review & Confirm"}];

  const overlay={position:"fixed",inset:0,zIndex:9999,background:"rgba(0,0,0,0.7)",display:"flex",alignItems:"flex-start",justifyContent:"center",padding:"40px 16px",overflowY:"auto"};
  const modal={background:dark?"#131625":"#fff",border:`0.5px solid ${dark?"#2d3147":"#e2e8f0"}`,borderRadius:"var(--border-radius-lg)",width:"100%",maxWidth:820,padding:"24px",position:"relative"};
  const inp={padding:"5px 8px",fontSize:12,fontFamily:"var(--font-mono)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",width:"100%",boxSizing:"border-box"};
  const sm={...inp,width:52,textAlign:"center"};

  return (
    <div style={overlay} onClick={e=>{if(e.target===e.currentTarget)onClose();}}>
      <div style={modal}>
        {/* Header */}
        <div style={{display:"flex",alignItems:"center",marginBottom:20}}>
          <div>
            <div style={{fontSize:14,fontWeight:500,marginBottom:2}}>Enter Game Result</div>
            <div style={{fontSize:11,color:"var(--color-text-secondary)"}}>Updates series result and player playoff stat totals simultaneously</div>
          </div>
          <button onClick={onClose} style={{marginLeft:"auto",padding:"4px 10px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>✕ Close</button>
        </div>

        {/* Step pills */}
        <div style={{display:"flex",gap:0,borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)",marginBottom:20,width:"fit-content"}}>
          {STEPS.map((st,i)=><button key={st.id} onClick={()=>setStep(st.id)} style={{
            padding:"6px 14px",fontSize:11,border:"none",borderRight:"0.5px solid var(--color-border-tertiary)",cursor:"pointer",
            background:step===st.id?"#3b82f6":"var(--color-background-secondary)",
            color:step===st.id?"white":"var(--color-text-secondary)",fontWeight:step===st.id?500:400}}>
            {i+1}. {st.l}
          </button>)}
        </div>

        {/* ── STEP 1: Game Result ── */}
        {step==="game"&&<div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,marginBottom:16}}>
            <div>
              <div style={{fontSize:11,color:"var(--color-text-secondary)",marginBottom:6}}>Series</div>
              <select value={seriesIdx} onChange={e=>{setSeriesIdx(+e.target.value);setGameIdx(0);}} style={{...inp}}>
                {allSeries.map((sr,i)=><option key={i} value={i}>{sr.homeAbbr&&sr.awayAbbr?`${sr.homeAbbr} vs ${sr.awayAbbr}`:`Series ${i+1}`}</option>)}
              </select>
            </div>
            <div>
              <div style={{fontSize:11,color:"var(--color-text-secondary)",marginBottom:6}}>Game Number</div>
              <select value={gameIdx} onChange={e=>setGameIdx(+e.target.value)} style={{...inp}}>
                {s?.games.map((_,i)=><option key={i} value={i}>Game {i+1}{s.games[i].result?" ✓":""}</option>)}
              </select>
            </div>
          </div>

          <div style={{padding:"12px 14px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",marginBottom:16}}>
            <div style={{fontSize:10,color:"var(--color-text-tertiary)",marginBottom:8,textTransform:"uppercase",letterSpacing:"0.08em"}}>
              Game {gameNum} — {HOME_PATTERN[gameNum]?homeName:awayName} hosts
            </div>
            <div style={{display:"flex",gap:12,alignItems:"center",flexWrap:"wrap"}}>
              {/* Winner */}
              <div>
                <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:4}}>Winner</div>
                <div style={{display:"flex",gap:6}}>
                  {[[homeName,"home"],[awayName,"away"]].map(([n,v])=>(
                    <button key={v} onClick={()=>setWinner(v)} style={{
                      padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",border:"0.5px solid",cursor:"pointer",fontWeight:winner===v?500:400,
                      borderColor:winner===v?"#10b981":"var(--color-border-secondary)",
                      background:winner===v?"rgba(16,185,129,0.15)":"var(--color-background-primary)",
                      color:winner===v?"#10b981":"var(--color-text-secondary)"}}>
                      {n}
                    </button>
                  ))}
                </div>
              </div>
              {/* Score */}
              <div>
                <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:4}}>{homeName} Score</div>
                <input type="number" min={0} max={20} value={homeScore} onChange={e=>setHomeScore(e.target.value)} style={{...sm}}/>
              </div>
              <div style={{alignSelf:"flex-end",paddingBottom:2,color:"var(--color-text-tertiary)",fontSize:14}}>–</div>
              <div>
                <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:4}}>{awayName} Score</div>
                <input type="number" min={0} max={20} value={awayScore} onChange={e=>setAwayScore(e.target.value)} style={{...sm}}/>
              </div>
              {/* OT */}
              <div style={{alignSelf:"flex-end",paddingBottom:4}}>
                <Toggle label="OT" checked={ot} onChange={v=>{setOt(v); if(!v) setOtScorer("");}}/>
              </div>
            </div>
            {/* v89: OT goal scorer — only shown when OT is checked AND winner is set.
                Filter pool to the winning team (OT goal must come from the team that won). */}
            {ot && winner && <div style={{marginTop:10,display:"flex",gap:8,alignItems:"center"}}>
              <span style={{fontSize:11,color:"var(--color-text-secondary)",minWidth:80}}>OT scorer:</span>
              <select value={otScorer} onChange={e=>setOtScorer(e.target.value)}
                style={{padding:"5px 8px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",minWidth:200}}>
                <option value="">— select player —</option>
                {pool.filter(p => p.team === (winner==="home"?homeAbbr:awayAbbr)).map(p => (
                  <option key={p.name} value={p.name}>{p.name}</option>
                ))}
              </select>
              <span style={{fontSize:9,color:"var(--color-text-tertiary)"}}>(used by First OT Scorer market)</span>
            </div>}
          </div>

          <div style={{display:"flex",gap:10,justifyContent:"flex-end"}}>
            <button onClick={()=>setStep("skaters")} disabled={!winner} style={{
              padding:"7px 20px",fontSize:12,borderRadius:"var(--border-radius-md)",border:"none",cursor:winner?"pointer":"default",
              background:winner?"#3b82f6":"var(--color-background-secondary)",color:winner?"white":"var(--color-text-tertiary)"}}>
              Next: Skater Stats →
            </button>
          </div>
        </div>}

        {/* ── STEP 2: Skater Stats ── */}
        {step==="skaters"&&<div>
          <div style={{display:"flex",gap:8,marginBottom:12,flexWrap:"wrap",alignItems:"center"}}>
            <input placeholder="Search player…" value={search} onChange={e=>setSearch(e.target.value)}
              style={{...inp,width:180}}/>
            <div style={{display:"flex",borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)"}}>
              {[["all","Both"],homeAbbr?[homeAbbr,homeName]:[null,null],awayAbbr?[awayAbbr,awayName]:[null,null]].filter(x=>x[0]).map(([v,l])=>(
                <button key={v} onClick={()=>setFilterTeam(v)} style={{
                  padding:"4px 10px",fontSize:11,border:"none",cursor:"pointer",
                  background:filterTeam===v?"#1d4ed8":"var(--color-background-secondary)",
                  color:filterTeam===v?"white":"var(--color-text-secondary)"}}>
                  {l}
                </button>
              ))}
            </div>
            <span style={{fontSize:10,color:"var(--color-text-tertiary)",marginLeft:"auto"}}>
              {Object.keys(playerDeltas).filter(k=>{const d=playerDeltas[k];return d.g||d.a||d.sog||d.hit||d.blk;}).length} players with stats · {totalGoals}G total
            </span>
          </div>
          <div style={{fontSize:10,color:"var(--color-text-tertiary)",marginBottom:8}}>
            Enter tonight's individual game stats — these are added to each player's cumulative playoff totals. Leave blank for players who didn't dress or had zeros.
          </div>
          <div style={{overflowX:"auto",maxHeight:380,overflowY:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead style={{position:"sticky",top:0,background:dark?"#131625":"#fff",zIndex:1}}>
                <tr style={{borderBottom:"0.5px solid var(--color-border-secondary)"}}>
                  {["Player","Team","G","A","SOG","HIT","BLK"].map((h,i)=>(
                    <th key={h} style={{padding:"5px 6px",textAlign:i<2?"left":"center",color:"var(--color-text-secondary)",fontWeight:500,fontSize:10,textTransform:"uppercase"}}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>{displayedPlayers.map((p,i)=>{
                const hasStats=getDelta(p.name,p.team,"g")||getDelta(p.name,p.team,"a")||getDelta(p.name,p.team,"sog")||getDelta(p.name,p.team,"hit")||getDelta(p.name,p.team,"blk");
                return (
                  <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:hasStats?(dark?"rgba(59,130,246,0.06)":"rgba(59,130,246,0.04)"):i%2===0?"transparent":(dark?"rgba(255,255,255,0.015)":"rgba(0,0,0,0.01)")}}>
                    <td style={{padding:"3px 6px",fontWeight:hasStats?500:400,fontSize:11}}>{p.name}</td>
                    <td style={{padding:"3px 6px"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
                    {["g","a","sog","hit","blk"].map(f=>(
                      <td key={f} style={{padding:"2px 4px",textAlign:"center"}}>
                        <input type="number" min={0} max={f==="sog"?20:f==="hit"?15:f==="blk"?10:5}
                          value={getDelta(p.name,p.team,f)||""}
                          placeholder="0"
                          onChange={e=>setDelta(p.name,p.team,f,e.target.value)}
                          style={{width:36,fontSize:11,textAlign:"center",padding:"2px 3px",fontFamily:"var(--font-mono)",
                            background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
                            borderRadius:3,color:"var(--color-text-primary)"}}/>
                      </td>
                    ))}
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
          <div style={{display:"flex",gap:10,justifyContent:"space-between",marginTop:12}}>
            <button onClick={()=>setStep("game")} style={{padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>← Back</button>
            <button onClick={()=>setStep("goalies")} style={{padding:"7px 20px",fontSize:12,borderRadius:"var(--border-radius-md)",border:"none",cursor:"pointer",background:"#3b82f6",color:"white"}}>Next: Goalie Saves →</button>
          </div>
        </div>}

        {/* ── STEP 3: Goalie Saves ── */}
        {step==="goalies"&&<div>
          <div style={{fontSize:11,color:"var(--color-text-tertiary)",marginBottom:12}}>
            Enter saves for tonight's starting goalies. Added to each goalie's cumulative playoff save totals.
          </div>
          {!goaliePool.length&&<div style={{color:"var(--color-text-secondary)",fontSize:12,padding:"12px 0"}}>
            No goalie data — load goalies.csv in the Upload tab to enable this step.
          </div>}
          {goaliePool.length>0&&<table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <TH cols={["Goalie","Team","Reg Sv/G","Share","Saves Tonight"]}/>
            <tbody>{goaliePool.map((g,i)=>(
              <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.015)":"rgba(0,0,0,0.01)")}}>
                <td style={{padding:"5px 8px",fontWeight:g.starter_share>0.4?500:400}}>{g.name}</td>
                <td style={{padding:"5px 8px"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(124,58,237,0.15)",color:"#a78bfa"}}>{g.team}</span></td>
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{g.saves_pg.toFixed(1)}</td>
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(g.starter_share*100).toFixed(0)}%</td>
                <td style={{padding:"3px 8px",textAlign:"center"}}>
                  <input type="number" min={0} max={60}
                    value={getGDelta(g.name,g.team,"saves")||""}
                    placeholder="—"
                    onChange={e=>setGDelta(g.name,g.team,"saves",e.target.value)}
                    style={{width:52,fontSize:12,textAlign:"center",padding:"3px 4px",fontFamily:"var(--font-mono)",
                      background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
                      borderRadius:3,color:"var(--color-text-primary)"}}/>
                </td>
              </tr>
            ))}</tbody>
          </table>}
          <div style={{display:"flex",gap:10,justifyContent:"space-between",marginTop:14}}>
            <button onClick={()=>setStep("skaters")} style={{padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>← Back</button>
            <button onClick={()=>setStep("review")} style={{padding:"7px 20px",fontSize:12,borderRadius:"var(--border-radius-md)",border:"none",cursor:"pointer",background:"#3b82f6",color:"white"}}>Review →</button>
          </div>
        </div>}

        {/* ── STEP 4: Review & Confirm ── */}
        {step==="review"&&<div>
          {/* Game summary */}
          <div style={{padding:"10px 14px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",marginBottom:14}}>
            <div style={{fontSize:10,textTransform:"uppercase",letterSpacing:"0.08em",color:"var(--color-text-tertiary)",marginBottom:6}}>Game Result</div>
            <div style={{display:"flex",gap:20,alignItems:"center",flexWrap:"wrap"}}>
              <div style={{fontSize:13,fontWeight:500}}>
                {homeName} <span style={{fontFamily:"var(--font-mono)",color:"#60a5fa"}}>{homeScore}</span>
                <span style={{color:"var(--color-text-tertiary)",margin:"0 6px"}}>–</span>
                <span style={{fontFamily:"var(--font-mono)",color:"#60a5fa"}}>{awayScore}</span> {awayName}
                {ot&&<span style={{fontSize:10,color:"#f59e0b",marginLeft:6}}>OT</span>}
              </div>
              <div style={{fontSize:11,color:"var(--color-text-secondary)"}}>
                Winner: <strong style={{color:"#10b981"}}>{winner==="home"?homeName:awayName}</strong>
              </div>
              <div style={{fontSize:10,color:"var(--color-text-tertiary)"}}>Series G{gameNum} · {homeAbbr||"?"} vs {awayAbbr||"?"}</div>
            </div>
          </div>

          {/* Skater summary */}
          {Object.values(playerDeltas).filter(d=>d.g||d.a||d.sog||d.hit||d.blk).length>0&&<div style={{marginBottom:14}}>
            <div style={{fontSize:10,textTransform:"uppercase",letterSpacing:"0.08em",color:"var(--color-text-tertiary)",marginBottom:6}}>
              Skater Stats ({Object.values(playerDeltas).filter(d=>d.g||d.a||d.sog||d.hit||d.blk).length} players · {totalGoals}G)
            </div>
            <div style={{maxHeight:200,overflowY:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <TH cols={["Player","Team","G","A","SOG","HIT","BLK"]}/>
                <tbody>{Object.values(playerDeltas).filter(d=>d.g||d.a||d.sog||d.hit||d.blk).map((d,i)=>(
                  <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                    <td style={{padding:"3px 8px",fontWeight:500}}>{d.name}</td>
                    <td style={{padding:"3px 8px"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{d.team}</span></td>
                    {["g","a","sog","hit","blk"].map(f=><td key={f} style={{padding:"3px 8px",textAlign:"center",fontFamily:"var(--font-mono)"}}>{d[f]||0}</td>)}
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </div>}

          {/* Goalie summary */}
          {Object.values(goalieDeltas).filter(d=>d.saves).length>0&&<div style={{marginBottom:14}}>
            <div style={{fontSize:10,textTransform:"uppercase",letterSpacing:"0.08em",color:"var(--color-text-tertiary)",marginBottom:6}}>
              Goalie Saves ({totalSaves} total)
            </div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <TH cols={["Goalie","Team","Saves"]}/>
              <tbody>{Object.values(goalieDeltas).filter(d=>d.saves).map((d,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                  <td style={{padding:"3px 8px",fontWeight:500}}>{d.name}</td>
                  <td style={{padding:"3px 8px"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(124,58,237,0.15)",color:"#a78bfa"}}>{d.team}</span></td>
                  <td style={{padding:"3px 8px",textAlign:"center",fontFamily:"var(--font-mono)",fontWeight:500}}>{d.saves}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>}

          <div style={{padding:"8px 12px",background:"rgba(16,185,129,0.08)",border:"0.5px solid rgba(16,185,129,0.25)",borderRadius:"var(--border-radius-md)",fontSize:11,color:"#10b981",marginBottom:14}}>
            On confirm: series G{gameNum} result updated · {Object.values(playerDeltas).filter(d=>d.g||d.a||d.sog||d.hit||d.blk).length} players' playoff totals incremented · all market prices reprice instantly.
          </div>

          <div style={{display:"flex",gap:10,justifyContent:"space-between"}}>
            <button onClick={()=>setStep("goalies")} style={{padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>← Back</button>
            <button onClick={handleCommit} disabled={!canCommit} style={{
              padding:"8px 28px",fontSize:13,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
              cursor:canCommit?"pointer":"default",
              background:canCommit?"#10b981":"var(--color-background-secondary)",
              color:canCommit?"white":"var(--color-text-tertiary)"}}>
              ✓ Confirm & Update All Markets
            </button>
          </div>
        </div>}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// LEADERS TAB
// ═══════════════════════════════════════════════════════════════════════════════
// v41: number input with local string state — only commits parent on blur or Enter.
// Fixes the laggy/cursor-jumpy behavior on the advancement table where every keystroke
// re-rendered the whole 16-team auto-chain.
// v76: text input with deferred commit. Used for team-name fields where every-keystroke
// state propagation triggered cascade re-renders (laggy on R2 with 4+ series mounted).
// Commits on blur or Enter — not on every keystroke.
function LazyText({value, onCommit, placeholder, transform, style={}}) {
  const [draft, setDraft] = useState(value || "");
  const [editing, setEditing] = useState(false);
  useEffect(()=>{
    if (!editing) setDraft(value || "");
  }, [value, editing]);
  const commit = () => {
    setEditing(false);
    const final = transform ? transform(draft) : draft;
    if (final !== value) onCommit(final);
    setDraft(final);
  };
  return <input type="text" placeholder={placeholder} value={draft}
    onFocus={()=>setEditing(true)}
    onChange={e=>setDraft(transform ? transform(e.target.value) : e.target.value)}
    onBlur={commit}
    onKeyDown={e=>{ if(e.key==="Enter") e.target.blur(); }}
    style={style}/>;
}

function LazyNI({value, onCommit, min, max, step=0.01, style={}, tabIndex, showSpinner=true}) {
  const [draft, setDraft] = useState(String(value));
  const [editing, setEditing] = useState(false);
  // Sync from prop when not actively editing (e.g., AUTO recompute updated the prop)
  useEffect(()=>{
    if (!editing) setDraft(String(value));
  }, [value, editing]);
  const commit = () => {
    setEditing(false);
    const parsed = parseFloat(draft);
    if (!isNaN(parsed)) {
      let v = parsed;
      if (min != null) v = Math.max(min, v);
      if (max != null) v = Math.min(max, v);
      onCommit(v);
      setDraft(String(v));
    } else {
      // invalid input — revert
      setDraft(String(value));
    }
  };
  // v49: stepper increments the current (or draft) value and commits immediately
  const stepBy = (delta) => {
    const cur = parseFloat(draft);
    const base = isNaN(cur) ? (value||0) : cur;
    let v = base + delta;
    // Round to step precision to avoid float drift (e.g., 0.1+0.1=0.200000001)
    const precision = Math.max(0, -Math.floor(Math.log10(Math.abs(step) || 1e-6)));
    v = +v.toFixed(precision);
    if (min != null) v = Math.max(min, v);
    if (max != null) v = Math.min(max, v);
    setDraft(String(v));
    setEditing(false);
    onCommit(v);
  };
  const inputEl = <input type="text" inputMode="decimal" value={draft}
    tabIndex={tabIndex}
    onFocus={()=>setEditing(true)}
    onChange={e=>setDraft(e.target.value)}
    onBlur={commit}
    onKeyDown={e=>{
      if (e.key==="Enter") { e.target.blur(); }
      else if (e.key==="Escape") { setDraft(String(value)); setEditing(false); e.target.blur(); }
      else if (e.key==="ArrowUp") { e.preventDefault(); stepBy(step); }
      else if (e.key==="ArrowDown") { e.preventDefault(); stepBy(-step); }
    }}
    style={{width:68,padding:"3px 6px",fontSize:12,fontFamily:"var(--font-mono)",
      background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
      borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",...style}}/>;
  if (!showSpinner) return inputEl;
  // Spinner variant: input flanked by compact ↑/↓ buttons (visible toggling)
  const btn = {padding:"0 5px",fontSize:10,lineHeight:1,cursor:"pointer",border:"0.5px solid var(--color-border-secondary)",background:"var(--color-background-secondary)",color:"var(--color-text-secondary)",height:24,display:"flex",alignItems:"center",justifyContent:"center"};
  return <span style={{display:"inline-flex",alignItems:"center",gap:2}}>
    {inputEl}
    <span style={{display:"inline-flex",flexDirection:"column",gap:1}}>
      <button type="button" tabIndex={-1} onClick={()=>stepBy(step)} style={{...btn,height:11,borderRadius:"3px 3px 0 0"}}>▲</button>
      <button type="button" tabIndex={-1} onClick={()=>stepBy(-step)} style={{...btn,height:11,borderRadius:"0 0 3px 3px"}}>▼</button>
    </span>
  </span>;
}

function LeadersTab({players,setPlayers,matchups,setMatchups,advancement,setAdvancement,globals,setGlobals,leaderMarket,STATS,lStat,setLStat,lScope,setLScope,lTopN,setLTopN,showTrue,setShowTrue,showDec,dark,allSeries,allSeriesByRound,simResultsBySeries,autoR1ByTeam,autoConfByTeam,autoCupByTeam,currentRound,setCurrentRound,linemates}) {
  const [showR1,setShowR1]=useState(false);
  const [showAdv,setShowAdv]=useState(false);
  // v39: advancement table entry mode — "prob" (0..1) or "decimal" (decimal odds, 1.01..)
  const [advEntryMode,setAdvEntryMode]=useState("decimal");
  const [filterTeam,setFilterTeam]=useState("ALL");
  const teams=[...new Set(leaderMarket.map(p=>p.team))].sort();
  const displayed=filterTeam==="ALL"?leaderMarket:leaderMarket.filter(p=>p.team===filterTeam);

  // v63: derive elimination state from played series across all rounds.
  // A team is "eliminated" if any of their series in any round shows the OTHER team reaching 4 wins.
  const eliminatedTeams = useMemo(()=>{
    const out = new Set();
    if (!allSeriesByRound) return out;
    for (const round of ["r1","r2","r3","f"]) {
      const arr = allSeriesByRound[round] || [];
      for (const s of arr) {
        if (!s || !s.homeAbbr || !s.awayAbbr) continue;
        let hw = 0, aw = 0;
        for (const g of (s.games||[])) {
          if (g.result === "home") hw++;
          else if (g.result === "away") aw++;
        }
        if (hw === 4) out.add(s.awayAbbr);
        else if (aw === 4) out.add(s.homeAbbr);
      }
    }
    return out;
  }, [allSeriesByRound]);

  // v63: when a team gets eliminated, zero their advancement probs and lock them as MANUAL=true
  // so downstream consumers (full-playoff leader markets, etc.) treat them as cup=conf=R1=0.
  useEffect(()=>{
    if (!eliminatedTeams || eliminatedTeams.size === 0) return;
    setAdvancement(prev => {
      let changed = false;
      const next = {...prev};
      for (const t of eliminatedTeams) {
        const cur = prev[t] || {winR1:0.5,winConf:0.25,winCup:0.1,manualR1:false,manualConf:false,manualCup:false};
        // Already zeroed and locked? skip
        if (cur.winR1 === 0 && cur.winConf === 0 && cur.winCup === 0 && cur.manualR1 && cur.manualConf && cur.manualCup) continue;
        next[t] = {...cur, winR1: 0, winConf: 0, winCup: 0, manualR1: true, manualConf: true, manualCup: true};
        changed = true;
      }
      return changed ? next : prev;
    });
  }, [eliminatedTeams, setAdvancement]);

  function updM(idx,f,v){setMatchups(prev=>{const u=[...prev];u[idx]={...u[idx],[f]:v};
    if(f==="homeWinPct"){const hw=v,aw=1-v;const p4=Math.pow(hw,4)+Math.pow(aw,4),p5=4*(Math.pow(hw,4)*aw+Math.pow(aw,4)*hw),p6=10*(Math.pow(hw,4)*aw*aw+Math.pow(aw,4)*hw*hw),p7=20*(Math.pow(hw,4)*aw*aw*aw+Math.pow(aw,4)*hw*hw*hw),tot=p4+p5+p6+p7;u[idx].expGames=tot>0?+((4*p4+5*p5+6*p6+7*p7)/tot).toFixed(2):5.82;}return u;});}

  return (
    <div>
      {/* v38: Round selector — same as Series Pricer. Affects which series feed leader markets. */}
      <div style={{display:"flex",gap:6,marginBottom:10,alignItems:"center"}}>
        <span style={{fontSize:10,fontWeight:500,letterSpacing:"0.1em",color:"var(--color-text-tertiary)",marginRight:6}}>ROUND</span>
        {ROUND_IDS.map(rid => (
          <button key={rid} onClick={()=>setCurrentRound&&setCurrentRound(rid)}
            style={{
              padding:"5px 14px",fontSize:11,fontWeight:500,borderRadius:"var(--border-radius-md)",cursor:"pointer",
              border:"0.5px solid",
              borderColor: currentRound===rid ? "#7c3aed" : "var(--color-border-secondary)",
              background: currentRound===rid ? "rgba(124,58,237,0.18)" : "var(--color-background-secondary)",
              color: currentRound===rid ? "#a78bfa" : "var(--color-text-secondary)",
            }}>
            {ROUND_LABELS[rid]}
          </button>
        ))}
      </div>
      <div style={{display:"flex",gap:10,alignItems:"center",flexWrap:"wrap",marginBottom:14}}>
        <Seg options={[{id:"r1",label:"Round 1"},{id:"r2",label:"Round 2"},{id:"full",label:"Full Playoff"}]} value={lScope} onChange={setLScope}/>
        <Seg options={STATS} value={lStat} onChange={setLStat} accent="#1d4ed8"/>
        <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)} style={SEL}>
          <option value="ALL">All Teams</option>
          {teams.map(t=><option key={t} value={t}>{t} – {TEAM_NAMES[t]||t}</option>)}
        </select>
        <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center"}}>
          Top <select value={lTopN} onChange={e=>setLTopN(+e.target.value)} style={SEL}>{[10,25,50,100].map(n=><option key={n} value={n}>{n}</option>)}</select>
        </label>
        <div style={{marginLeft:"auto",display:"flex",gap:10,alignItems:"center"}}>
          <span style={{fontSize:9,padding:"2px 6px",borderRadius:3,background:"rgba(124,58,237,0.15)",color:"#a78bfa",letterSpacing:0.4,fontWeight:500}} title={lScope==="r1"?"Per-matchup unified series sim (5k each) + cross-matchup leader sim (20k). Teammate correlation within matchup.":"Independent-player NB leader sim (10k)."}>{lScope==="r1"?"UNIFIED":"MC 10K"}</span>
          <Toggle label="True %" checked={showTrue} onChange={setShowTrue}/>
        </div>
      </div>

      <div style={{display:"flex",gap:14,marginBottom:12,flexWrap:"wrap",alignItems:"center",padding:"7px 12px",
        background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",border:"0.5px solid var(--color-border-tertiary)"}}>
        {[{k:lScope==="r1"?"overroundR1":"overroundFull",l:"Overround",min:1,max:1.5,step:0.01},{k:"powerFactor",l:"Power Factor",min:0.5,max:2,step:0.05},{k:"rateDiscount",l:"Rate Discount",min:0.5,max:1,step:0.01}].map(({k,l,min,max,step})=>(
          <label key={k} style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center"}}>
            {l}: <LazyNI value={globals[k]} onCommit={v=>setGlobals(g=>({...g,[k]:v}))} min={min} max={max} step={step} style={{width:56}}/>
          </label>
        ))}
        {[["⚙ R1 Matchups",showR1,setShowR1],[lScope==="full"?"⚙ Advancement":null,showAdv,setShowAdv]].filter(x=>x[0]).map(([l,s,set])=>(
          <button key={l} onClick={()=>set(v=>!v)} style={{padding:"4px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",cursor:"pointer",
            background:s?"#1d4ed820":"var(--color-background-secondary)",border:s?"0.5px solid #3b82f6":"0.5px solid var(--color-border-secondary)",
            color:s?"#60a5fa":"var(--color-text-secondary)"}}>{l}</button>
        ))}
      </div>

      {showR1&&<Card style={{marginBottom:14}}>
        <SH title="Round 1 Matchups"/>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(300px,1fr))",gap:10}}>
          {matchups.map((m,idx)=>(
            <div key={idx} style={{padding:10,border:"0.5px solid var(--color-border-tertiary)",borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)"}}>
              <div style={{fontSize:9,fontWeight:500,color:"var(--color-text-tertiary)",marginBottom:6,textTransform:"uppercase"}}>Series {idx+1}</div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 66px",gap:4,marginBottom:6}}>
                {[["homeTeam","Home team"],["homeAbbr","Abbr"],["awayTeam","Away team"],["awayAbbr","Abbr"]].map(([f,ph])=>(
                  <LazyText key={f} placeholder={ph} value={m[f]||""}
                    onCommit={v=>updM(idx,f,v)}
                    transform={f.includes("Abbr") ? (x=>x.toUpperCase()) : null}
                    style={{padding:"3px 6px",fontSize:11,background:"var(--color-background-primary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}/>
                ))}
              </div>
              <div style={{display:"flex",gap:6,fontSize:11,alignItems:"center"}}>
                <span style={{color:"var(--color-text-secondary)"}}>Win%</span>
                <LazyNI value={m.homeWinPct} onCommit={v=>updM(idx,"homeWinPct",v)} min={0} max={1} step={0.01} style={{width:52}}/>
                {(()=>{
                  const hs=m.homeAbbr?computeTeamStrength(players,m.homeAbbr):null;
                  const as=m.awayAbbr?computeTeamStrength(players,m.awayAbbr):null;
                  const auto=(hs&&as)?winProbFromStrength(hs,as,0.05,1.0):null;
                  if(auto==null) return null;
                  return <button onClick={()=>updM(idx,"homeWinPct",+auto.toFixed(3))}
                    style={{fontSize:9,padding:"2px 6px",background:"rgba(59,130,246,0.12)",border:"0.5px solid #3b82f6",borderRadius:3,color:"#60a5fa",cursor:"pointer"}}
                    title={`${m.homeAbbr} Δ=${hs.diff60.toFixed(3)} vs ${m.awayAbbr} Δ=${as.diff60.toFixed(3)}`}>
                    xG: {(auto*100).toFixed(0)}%
                  </button>;
                })()}
                <span style={{color:"var(--color-text-secondary)"}}>Total</span>
                <LazyNI value={m.expTotal} onCommit={v=>updM(idx,"expTotal",v)} min={3} max={12} step={0.1} style={{width:48}}/>
                <span style={{fontSize:10,color:"var(--color-text-tertiary)",marginLeft:"auto"}}>Exp {m.expGames}g</span>
              </div>
              <div style={{display:"flex",gap:6,fontSize:11,alignItems:"center",marginTop:4}}>
                <span style={{color:"var(--color-text-secondary)"}}>Live H/A:</span>
                <LazyNI value={m.homeWins||0} onCommit={v=>updM(idx,"homeWins",Math.round(v))} min={0} max={4} step={1} style={{width:36}}/>
                <span>–</span>
                <LazyNI value={m.awayWins||0} onCommit={v=>updM(idx,"awayWins",Math.round(v))} min={0} max={4} step={1} style={{width:36}}/>
              </div>
            </div>
          ))}
        </div>
      </Card>}

      {showAdv&&lScope==="full"&&<Card style={{marginBottom:14}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",gap:12,marginBottom:8,flexWrap:"wrap"}}>
          <SH title="Team Advancement" sub="P(Win R1) auto from Series Pricer; Conf/Cup chained through bracket (sim → xG fallback)."/>
          <label style={{display:"flex",gap:6,alignItems:"center",fontSize:11,color:"var(--color-text-secondary)"}}>
            Entry mode:
            <select value={advEntryMode} onChange={e=>setAdvEntryMode(e.target.value)}
              style={{padding:"4px 10px",fontSize:11,fontWeight:500,borderRadius:"var(--border-radius-md)",cursor:"pointer",
                background:"var(--color-background-secondary)",border:"0.5px solid #3b82f6",color:"#60a5fa"}}>
              <option value="prob">Probability</option>
              <option value="decimal">Decimal Odds</option>
            </select>
          </label>
        </div>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <TH cols={["Team", advEntryMode==="prob"?"P(Win R1)":"R1 Decimal", advEntryMode==="prob"?"P(Win Conf)":"Conf Decimal", advEntryMode==="prob"?"P(Win Cup)":"Cup Decimal"]}/>
          <tbody>{PLAYOFF_TEAMS.map((t,rowIdx)=>{
            const adv=advancement[t]||{winR1:0.5,winConf:0.25,winCup:0.1,manualR1:false,manualConf:false,manualCup:false};
            const autoR1 = autoR1ByTeam[t];
            const autoConf = (autoConfByTeam||{})[t];
            const autoCup = (autoCupByTeam||{})[t];
            // Manual flags — once user types, that field locks to stored value until unlocked
            const useManR1 = !!adv.manualR1, useManConf = !!adv.manualConf, useManCup = !!adv.manualCup;
            const r1Prob = (autoR1 != null && !useManR1) ? autoR1 : adv.winR1;
            const confProb = (autoConf != null && !useManConf) ? autoConf : adv.winConf;
            const cupProb = (autoCup != null && !useManCup) ? autoCup : adv.winCup;
            const probToDec = (p) => p > 0.0001 ? +(1/p).toFixed(2) : 50000;
            const decToProb = (d) => d > 1.001 ? Math.min(0.9999, 1/d) : 0.9999;
            // v49: column-major Tab ordering — R1 col gets indexes 101..100+N, Conf 201..200+N, Cup 301..300+N.
            //      Adding 100 base to avoid conflict with other tabbable elements on the page.
            const N = PLAYOFF_TEAMS.length;
            const tabR1 = 101 + rowIdx;
            const tabConf = 201 + rowIdx;
            const tabCup = 301 + rowIdx;
            const renderCell = (probVal, autoAvail, useMan, fieldKey, manualKey, autoTitle, tabIdx) => {
              const isAuto = autoAvail && !useMan;
              const setProbAndLock = (newProb) => setAdvancement(p=>({
                ...p,
                [t]: {...(p[t]||{winR1:0.5,winConf:0.25,winCup:0.1,manualR1:false,manualConf:false,manualCup:false}),
                  [fieldKey]: newProb, [manualKey]: true}
              }));
              const unlock = () => setAdvancement(p=>({
                ...p,
                [t]: {...(p[t]||{winR1:0.5,winConf:0.25,winCup:0.1,manualR1:false,manualConf:false,manualCup:false}),
                  [manualKey]: false}
              }));
              return (
                <div style={{display:"flex",gap:4,alignItems:"center",justifyContent:"flex-end"}}>
                  {isAuto && <span style={{fontSize:9,padding:"1px 5px",background:"rgba(59,130,246,0.15)",color:"#60a5fa",borderRadius:3,letterSpacing:0.3}} title={autoTitle}>AUTO</span>}
                  {useMan && <span style={{fontSize:9,padding:"1px 5px",background:"rgba(245,158,11,0.15)",color:"#f59e0b",borderRadius:3,letterSpacing:0.3}} title="manual override">MANUAL</span>}
                  {advEntryMode==="decimal"
                    ? <LazyNI value={probToDec(probVal)} onCommit={d=>setProbAndLock(decToProb(d))} min={1.01} max={1000} step={0.01} style={{width:64}} tabIndex={tabIdx} showSpinner={true}/>
                    : <LazyNI value={+probVal.toFixed(3)} onCommit={v=>setProbAndLock(v)} min={0} max={1} step={0.01} style={{width:58}} tabIndex={tabIdx} showSpinner={true}/>}
                  {useMan && autoAvail && (
                    <button type="button" onClick={unlock} title="Revert to AUTO" tabIndex={-1}
                      style={{padding:"2px 5px",fontSize:10,lineHeight:1,borderRadius:3,cursor:"pointer",
                        background:"rgba(100,116,139,0.10)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)"}}>↻</button>
                  )}
                </div>
              );
            };
            return (
            <tr key={t} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",
              background: eliminatedTeams.has(t) ? (dark?"rgba(239,68,68,0.06)":"rgba(239,68,68,0.04)") : "transparent",
              opacity: eliminatedTeams.has(t) ? 0.6 : 1}}>
              <td style={{padding:"4px 8px",fontWeight:500}}>
                {t} <span style={{color:"var(--color-text-secondary)",fontWeight:400}}>{TEAM_NAMES[t]}</span>
                {eliminatedTeams.has(t) && <span style={{marginLeft:8,fontSize:9,padding:"1px 6px",borderRadius:3,background:"rgba(239,68,68,0.18)",color:"#f87171",fontWeight:500,letterSpacing:0.3}}>ELIMINATED</span>}
              </td>
              {eliminatedTeams.has(t) ? (
                <>
                  <td colSpan={3} style={{padding:"3px 6px",textAlign:"right",fontSize:10,color:"var(--color-text-tertiary)",fontStyle:"italic"}}>
                    Lost series · all advancement = 0
                  </td>
                </>
              ) : (
                <>
                  <td style={{padding:"3px 6px",textAlign:"right"}}>{renderCell(r1Prob, autoR1!=null, useManR1, "winR1", "manualR1", "auto from Series Pricer", tabR1)}</td>
                  <td style={{padding:"3px 6px",textAlign:"right"}}>{renderCell(confProb, autoConf!=null, useManConf, "winConf", "manualConf", "chained R1 → R2 → R3 (sim or xG)", tabConf)}</td>
                  <td style={{padding:"3px 6px",textAlign:"right"}}>{renderCell(cupProb, autoCup!=null, useManCup, "winCup", "manualCup", "chained R1 → R2 → R3 → F (sim or xG)", tabCup)}</td>
                </>
              )}
            </tr>);})}</tbody>
        </table>
      </Card>}

      {!players?<Card style={{textAlign:"center",padding:"40px"}}>
        <div style={{fontSize:13,color:"var(--color-text-secondary)",marginBottom:6}}>No player data</div>
        <div style={{fontSize:11,color:"var(--color-text-tertiary)"}}>Upload tab → Load skaters.csv</div>
      </Card>:<Card>
        <div style={{display:"flex",gap:10,alignItems:"center",marginBottom:10}}>
          <span style={{fontSize:10,fontWeight:500,textTransform:"uppercase",letterSpacing:"0.08em",color:"var(--color-text-secondary)"}}>
            {lScope==="r1"?"R1":lScope==="r2"?"R2":"Playoff"} {STATS.find(s=>s.id===lStat)?.label} Leader
          </span>
          <span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>{displayed.length} shown</span>
        </div>
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <TH cols={["#","Player","Team","Role","Now","λ",...(showTrue?["True%"]:[]),"Adj%","American",...(showDec?["Dec"]:[])]}/>
            <tbody>{displayed.map((p,i)=>{
              const rank=leaderMarket.indexOf(p)+1,a=toAmer(p.adjProb);
              const now=readActual(p, lStat, lScope==="r1" ? "r1" : lScope==="r2" ? "r2" : "full");
              return <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                <td style={{padding:"4px 8px",color:"var(--color-text-tertiary)",fontSize:10,width:28}}>{rank}</td>
                <td style={{padding:"4px 8px",fontWeight:rank<=3?500:400}}>{p.name}</td>
                <td style={{padding:"4px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 5px",borderRadius:3,background:"rgba(59,130,246,0.12)",color:"#60a5fa",fontWeight:500}}>{p.team}</span></td>
                <td style={{padding:"4px 8px",textAlign:"right"}}><RoleBadge role={p.lineRole}/></td>
                <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:now>0?500:400,color:now>0?"#4ade80":"var(--color-text-tertiary)"}}>{now>0?now:"—"}</td>
                <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{p.lambda.toFixed(2)}</td>
                {showTrue&&<td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(p.trueProb*100).toFixed(2)}%</td>}
                <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(p.adjProb*100).toFixed(2)}%</td>
                <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:a<0?"#4ade80":"var(--color-text-primary)"}}>{a>0?`+${a}`:a}</td>
                {showDec&&<td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{toDec(p.adjProb).toFixed(2)}</td>}
              </tr>;
            })}</tbody>
          </table>
        </div>
      </Card>}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// SERIES TAB
// ═══════════════════════════════════════════════════════════════════════════════
function SeriesTab({allSeries,setAllSeries,players,goalies,margins,setMargins,globals,showTrue,dark,onEnterGame,gameUploadCounter,simResultsBySeries,setSimForSeries,currentRound,setCurrentRound,linemates}) {
  const [si,setSi]=useState(0);
  // v38: reset to series 0 when round changes (different rounds have different series counts)
  useEffect(()=>{
    setSi(0);
  }, [currentRound]);
  // Defensive: clamp si if it exceeds available series
  const safeSi = Math.min(si, Math.max(0, (allSeries.length||1) - 1));
  const [mkt,setMkt]=useState("winner");
  const [showMgn,setShowMgn]=useState(false);
  const [showIRPanel,setShowIRPanel]=useState(false);
  const s=allSeries[safeSi] || defaultSeries(0);

  // v83: per-series margin overrides. Each series can hold its own override values; everything
  // not overridden inherits from the global `margins` (Settings tab). Edits here are local
  // to this series; edits on Settings tab still affect every series that hasn't overridden.
  const effMargins = useMemo(()=>({...margins, ...(s.marginOverrides||{})}), [margins, s.marginOverrides]);
  function updMarginOverride(key, val) {
    setAllSeries(p=>{
      const u=[...p];
      const cur = u[safeSi] || {};
      const ovr = {...(cur.marginOverrides||{})};
      // Empty/null/NaN → remove the override (revert to inherit). Otherwise store the value
      // even if it equals the current global — user might want it to be sticky.
      if (val == null || val === "" || isNaN(val)) {
        delete ovr[key];
      } else {
        ovr[key] = val;
      }
      u[safeSi] = {...cur, marginOverrides: Object.keys(ovr).length ? ovr : undefined};
      return u;
    });
  }

  // v24: compute team strengths from on-ice xG for auto win% generation
  const homeStrength = useMemo(()=>s.homeAbbr?computeTeamStrength(players,s.homeAbbr):null, [players,s.homeAbbr]);
  const awayStrength = useMemo(()=>s.awayAbbr?computeTeamStrength(players,s.awayAbbr):null, [players,s.awayAbbr]);
  const autoWinPct = useMemo(()=>{
    if(!homeStrength||!awayStrength) return null;
    return winProbFromStrength(homeStrength,awayStrength,0.05,1.0);
  },[homeStrength,awayStrength]);

  function updS(f,v){setAllSeries(p=>{const u=[...p];u[safeSi]={...u[safeSi],[f]:v};return u;});}
  function updG(gi,f,v){setAllSeries(p=>{const u=[...p],games=[...u[safeSi].games];
    // v22: setting pOT directly flags it as manual
    if(f==="pOT") games[gi]={...games[gi],pOT:v,pOT_manual:true};
    else games[gi]={...games[gi],[f]:v};
    if(gi===0&&f==="winPct")for(let i=1;i<7;i++)if(games[i].winPct===null)games[i]={...games[i],winPct:HOME_PATTERN[i+1]?v:1-v};
    if(gi===0&&f==="expTotal")for(let i=1;i<7;i++)if(games[i].expTotal===null)games[i]={...games[i],expTotal:v};
    if(gi===0&&f==="pOT")for(let i=1;i<7;i++)if(!games[i].pOT_manual)games[i]={...games[i],pOT:v,pOT_manual:true};
    u[safeSi]={...u[safeSi],games};return u;});}

  const effG=s.games.map((g,i)=>{
    const winPct = g.winPct??(HOME_PATTERN[i+1]?(s.games[0].winPct||0.55):1-(s.games[0].winPct||0.55));
    const expTotal = g.expTotal??(s.games[0].expTotal||5.5);
    // v22: auto-compute pOT from expTotal+winPct unless user has manually overridden (pOT_manual flag set)
    const pOT = g.pOT_manual ? (g.pOT??0.22) : pOTGame(expTotal, winPct);
    return {...g, winPct, expTotal, pOT};
  });
  const effKey=JSON.stringify(effG)+JSON.stringify({sr:s.shutoutRate,wgs:s.winnerGoalShift});

  const outcomes=useMemo(()=>computeOutcomes(effG),[effKey]);
  const hwp=["4-0","4-1","4-2","4-3"].reduce((acc,k)=>acc+(outcomes[k]||0),0);
  const awp=1-hwp;
  const [adjH,adjA]=applyMargin([hwp,awp],effMargins.winner);

  const len4=(outcomes["4-0"]||0)+(outcomes["0-4"]||0);
  const len5=(outcomes["4-1"]||0)+(outcomes["1-4"]||0);
  const len6=(outcomes["4-2"]||0)+(outcomes["2-4"]||0);
  const len7=(outcomes["4-3"]||0)+(outcomes["3-4"]||0);
  const tot=len4+len5+len6+len7;
  const expG=tot>0?(4*len4+5*len5+6*len6+7*len7)/tot:5.82;

  // v31 (hoisted): realized state of this series — used to mark settled outcomes across all market panels.
  // MUST be declared before any market useMemo that references it (TDZ).
  const realized = useMemo(()=>{
    let hw=0, aw=0, gH=0, gA=0, sh=0, ot=0, played=0;
    for (const g of effG) {
      if (g.result === "home") { hw++; played++; }
      else if (g.result === "away") { aw++; played++; }
      if (typeof g.homeScore === "number" && typeof g.awayScore === "number") {
        gH += g.homeScore; gA += g.awayScore;
        if (g.homeScore === 0 || g.awayScore === 0) sh++;
      }
      if (g.wentOT || g.ot || g.result === "ot") ot++;
    }
    return {hw, aw, goalsH:gH, goalsA:gA, shutouts:sh, otGames:ot, gamesPlayed:played, gamesRemaining:Math.max(0, 7-played), seriesOver: hw>=4 || aw>=4};
  }, [effKey]);

  // v81: when a series outcome reduces to "winner of the next single game", price it AT
  // the next-game money line (winPct × winner overround) rather than applying series-market juice
  // on top. Otherwise: same outcome shows up at different prices in different markets, which is
  // a sharp magnet (e.g. correct-score "PHI 4-2" and win-order "PHI wins G6" should be identical
  // when the series is at 3-2 PHI — they're literally the same event).
  //
  // Returns null if the outcome doesn't collapse to a single next-game result; otherwise the
  // adjusted (with-margin) price using effMargins.winner as the per-game juice.
  //
  // collapseSide: "home" or "away" — which team's next-game win produces this outcome.
  const nextGameCollapsePrice = useCallback((collapseSide) => {
    // Find the next unplayed game
    const nextG = effG.find(g => !g.result);
    if (!nextG) return null;
    const wp = nextG.winPct;
    if (wp == null) return null;
    const truePrice = collapseSide === "home" ? wp : (1 - wp);
    const adj = Math.min(0.995, truePrice * (effMargins.winner || 1.04));
    return adj;
  }, [effG, effMargins.winner]);
  // Test: does (finalHw, finalAw) require exactly one specific next-game result given current state?
  // Returns "home" / "away" / null.
  const collapseSideForScore = useCallback((finalHw, finalAw) => {
    if (realized.seriesOver) return null;
    // Home clinches via this outcome: home=4, away unchanged from realized
    if (finalHw === 4 && realized.hw === 3 && finalAw === realized.aw) return "home";
    // Away clinches via this outcome
    if (finalAw === 4 && realized.aw === 3 && finalHw === realized.hw) return "away";
    return null;
  }, [realized]);

  // v31 (hoisted): helper — derive O/U rows from a sim PMF, with settled detection.
  function ouFromSimPMF(pmf, lines, marginVal, realizedCount=0, maxAdditional=Infinity) {
    return lines.map(line => {
      const lineCeil = Math.ceil(line - 0.001);
      const settledOver = realizedCount > line;
      const settledUnder = (realizedCount + maxAdditional) < lineCeil;
      let pOver, pUnder;
      if (settledOver) { pOver = 1; pUnder = 0; }
      else if (settledUnder) { pOver = 0; pUnder = 1; }
      else { pOver = pAtLeast(pmf, lineCeil); pUnder = 1 - pOver; }
      let [ao, au] = (settledOver || settledUnder)
        ? [pOver, pUnder]
        : applyMargin([pOver, pUnder], marginVal);
      // v34: if margin pushes either side >= 100%, the line is effectively dead — treat as settled.
      let extraSettledOver = false, extraSettledUnder = false;
      if (!settledOver && !settledUnder) {
        if (ao >= 1.0) { extraSettledOver = true; ao = 1; au = 0; }
        else if (au >= 1.0) { extraSettledUnder = true; au = 1; ao = 0; }
      }
      const settled = settledOver || settledUnder || extraSettledOver || extraSettledUnder;
      const settledSide = (settledOver || extraSettledOver) ? "over" : (settledUnder || extraSettledUnder) ? "under" : null;
      return {line, pOver, pUnder, ao, au, _settled: settled, _settledSide: settledSide};
    });
  }
  function sortSettled(rows) {
    return [...rows].sort((a,b) => {
      if (!!a._settled !== !!b._settled) return a._settled ? 1 : -1;
      return (a.line ?? 0) - (b.line ?? 0);
    });
  }

  // v16 fix: per-game effective weight.
  // v23: extended with goalie-faced multiplier for scoring stats.
  // For scoring stats, the opposing goalie's quality scales the game's contribution.
  // For defensive stats (hits/blk/pim/tk/give), no goalie adjustment — those are independent of who's in net.
  const BASELINE_GAME_GOALS = 5.8;
  const pGamePlayed = [null,1,1,1,1, 1-len4, len6+len7, len7];

  // v23: helper to resolve which goalie is in net for a team in a given game.
  // Preference: (1) game's manual override, (2) team's starter (highest starter_share among non-BACKUP/SCRATCHED)
  const goalieFor = (teamAbbr, game) => {
    if (!goalies || !teamAbbr) return null;
    const manualName = game.homeGoalie && teamAbbr===s.homeAbbr ? game.homeGoalie
                     : game.awayGoalie && teamAbbr===s.awayAbbr ? game.awayGoalie
                     : null;
    if (manualName) {
      const g = goalies.find(x=>x.name===manualName && x.team===teamAbbr);
      if (g) return g;
    }
    // v92: filter out IR/CUT goalies; prefer STARTER role over starter_share alone.
    const teamGoalies = goalies.filter(g=>{
      if (g.team !== teamAbbr) return false;
      const r = canonicalRole(g.lineRole);
      return r !== "IR" && r !== "CUT";
    });
    if (!teamGoalies.length) return null;
    const starter = teamGoalies.find(g => canonicalRole(g.lineRole) === "STARTER");
    if (starter) return starter;
    return teamGoalies.reduce((best,g)=>(!best||g.starter_share>best.starter_share)?g:best, null);
  };

  // Build a per-game scalar of goalie quality FACED BY each team.
  // homeTeam faces awayTeam's goalie, awayTeam faces homeTeam's goalie.
  // v92: also stash `faceByHome` / `faceByAway` directly on each effG entry so downstream
  // closed-form functions (computeShutoutPMF, computeTeamGoalsPMF, seriesWinProbs) can read
  // them without needing a separate goalieQualityFaced array passed through. Previously these
  // functions ignored opposing-goalie quality entirely — only the player-level rate
  // (gameEquivalentsFor) used it. That meant Total Goals / Shutouts / Most Goals were all
  // priced as if both teams faced average goalies.
  const goalieQualityFaced = effG.map((g,i) => {
    const hGoalie = goalieFor(s.awayAbbr, g); // home players face this goalie
    const aGoalie = goalieFor(s.homeAbbr, g); // away players face this goalie
    const faceByHome = hGoalie ? 1/(hGoalie.quality ?? 1) : 1.0;
    const faceByAway = aGoalie ? 1/(aGoalie.quality ?? 1) : 1.0;
    // Mutate the effG entry so closed-form consumers can use it
    g.faceByHome = faceByHome;
    g.faceByAway = faceByAway;
    return { faceByHome, faceByAway };
  });

  // Scalar gameEquivalents (no goalie adjustment) — kept for legacy callers and non-scoring stats
  let gameEquivalents = 0;
  for(let i=0;i<7;i++){
    const g = effG[i];
    if(g.result) continue;
    const p = pGamePlayed[i+1] ?? 0;
    const scale = (g.expTotal || BASELINE_GAME_GOALS) / BASELINE_GAME_GOALS;
    gameEquivalents += p * scale;
  }

  // v62: Realized scoring multiplier — adjust future player lambdas based on actual goals scored vs expected so far.
  //   "off"      → no adjustment (default)
  //   "combined" → uniform multiplier = realized_total/game ÷ expected_total/game (same for both teams)
  //   "perTeam"  → separate multipliers for home and away teams, computed from each team's actual goals
  // Multipliers clamped to [0.6, 1.4] to avoid wild swings on small samples (1-2 played games).
  // Helps adapt to environments where goalies are hot/cold, or one team is shooting/converting unusually.
  const realizedAdjMode = s.realizedAdjMode || "off";
  const realizedAdj = useMemo(()=>{
    if (realizedAdjMode === "off") return { home: 1.0, away: 1.0, combined: 1.0, gp: 0 };
    let realizedHome = 0, realizedAway = 0, expectedTotal = 0, expectedHomeGoals = 0, expectedAwayGoals = 0, gp = 0;
    for (const g of effG) {
      if (!g.result) continue;
      gp += 1;
      realizedHome += Number(g.homeScore) || 0;
      realizedAway += Number(g.awayScore) || 0;
      // Expected per-game uses input expTotal split by goal-share formula
      const total = g.expTotal || 5.82;
      const goalShare = 0.5 + (g.winPct - 0.5) * 0.60;
      expectedHomeGoals += total * goalShare;
      expectedAwayGoals += total * (1 - goalShare);
      expectedTotal += total;
    }
    if (gp === 0) return { home: 1.0, away: 1.0, combined: 1.0, gp: 0 };
    const clamp = (x) => Math.max(0.6, Math.min(1.4, x));
    const realizedTotal = realizedHome + realizedAway;
    const home = expectedHomeGoals > 0 ? clamp(realizedHome / expectedHomeGoals) : 1.0;
    const away = expectedAwayGoals > 0 ? clamp(realizedAway / expectedAwayGoals) : 1.0;
    const combined = expectedTotal > 0 ? clamp(realizedTotal / expectedTotal) : 1.0;
    return { home, away, combined, gp, realizedHome, realizedAway, expectedHomeGoals, expectedAwayGoals };
  }, [effKey, realizedAdjMode]);

  // v23: player-team-aware gameEquivalents function.
  // For scoring stats, applies the per-game goalie-quality-faced multiplier.
  // For non-scoring, returns the scalar gameEquivalents.
  const gameEquivalentsFor = (playerTeam, stat) => {
    if (!SCORING_STATS.has(stat)) return gameEquivalents;
    let total = 0;
    for (let i=0; i<7; i++) {
      const g = effG[i];
      if (g.result) continue;
      const p = pGamePlayed[i+1] ?? 0;
      const scale = (g.expTotal || BASELINE_GAME_GOALS) / BASELINE_GAME_GOALS;
      const goalieMult = playerTeam===s.homeAbbr ? goalieQualityFaced[i].faceByHome
                       : playerTeam===s.awayAbbr ? goalieQualityFaced[i].faceByAway
                       : 1.0;
      // v62: apply realized-scoring adjustment for player's team
      let realizedMult = 1.0;
      if (realizedAdjMode === "perTeam") {
        realizedMult = playerTeam === s.homeAbbr ? realizedAdj.home : playerTeam === s.awayAbbr ? realizedAdj.away : 1.0;
      } else if (realizedAdjMode === "combined") {
        realizedMult = realizedAdj.combined;
      }
      total += p * scale * goalieMult * realizedMult;
    }
    return total;
  };

  // v38: sim cache key now includes round prefix so PIT in R1 vs PIT in R2 don't collide.
  const seriesKey = (currentRound||"r1") + "|" + (s.homeAbbr||"") + "|" + (s.awayAbbr||"");
  const cached = simResultsBySeries[seriesKey];
  const simResult = cached ? cached.result : null;
  const simKey = cached ? cached.key : null;
  const [simRunning, setSimRunning] = useState(false);
  const simStale = simResult && simKey !== effKey+"|"+seriesKey;
  const runSim = useCallback(()=>{
    if (!players || !s.homeAbbr || !s.awayAbbr) return;
    setSimRunning(true);
    setTimeout(()=>{
      try {
        const inputs = buildSimInputs(effG, s.homeAbbr, s.awayAbbr, players, globals, goalieQualityFaced, pGamePlayed, linemates, currentRound);
        if (!inputs.pool.length) { setSimForSeries(seriesKey, null); setSimRunning(false); return; }
        const t0 = (typeof performance!=="undefined"?performance.now():Date.now());
        const result = simulateSeries(inputs, globals.dispersion, 20000, 31337);
        const t1 = (typeof performance!=="undefined"?performance.now():Date.now());
        setSimForSeries(seriesKey, {
          result: { ...result, simMs: Math.round(t1-t0) },
          key: effKey+"|"+seriesKey,
          ts: Date.now(),
        });
      } finally {
        setSimRunning(false);
      }
    }, 30);
  }, [effKey, s.homeAbbr, s.awayAbbr, players, goalies, globals, seriesKey, setSimForSeries, currentRound]);

  // v34: NO LONGER clear sim on series switch. The per-series cache means each series
  // keeps its own sim result. Switching back retrieves it. Only re-run on explicit click.
  // (Previous behaviour: setSimResult(null) on s.homeAbbr/s.awayAbbr/si change — REMOVED.)

  // v43: "Run All Sims" — sequentially sims every series in the current round. Simple
  // implementation: switch si, wait for runSim to settle, advance. ~5s per series.
  const [runAllProgress, setRunAllProgress] = useState(null); // {current, total} or null
  const runAllSims = useCallback(()=>{
    if (!players || runAllProgress) return;
    const eligible = (allSeries||[]).map((sr,i)=>({sr,i})).filter(x=>x.sr.homeAbbr && x.sr.awayAbbr);
    if (!eligible.length) return;
    setRunAllProgress({current:0, total:eligible.length});
    let stepIdx = 0;
    const stepNext = () => {
      if (stepIdx >= eligible.length) {
        setRunAllProgress(null);
        return;
      }
      const targetSi = eligible[stepIdx].i;
      setSi(targetSi);
      setRunAllProgress({current: stepIdx+1, total: eligible.length});
      stepIdx++;
      // Wait for the si change to propagate + runSim to fire (auto-run effect already exists for upload counter
      // but not for si change — explicit trigger needed). Defer past the React commit + the runSim 30ms internal delay.
      setTimeout(()=>{
        // Trigger sim for the now-current series
        try { runSim(); } catch(e) { /* swallow */ }
        // Wait for sim to complete (default ~5s) before next series
        setTimeout(stepNext, 6000);
      }, 100);
    };
    stepNext();
  }, [players, allSeries, runAllProgress, runSim]);

  // v31: auto-run sim after a game upload commit. Watches the App-level counter that
  // GameStatImporter bumps. Only fires if we have everything we need (players + both teams set).
  // Skipped on initial mount (counter starts at 0 and we use a ref to detect changes).
  const lastUploadSeen = useRef(0);
  useEffect(()=>{
    if (gameUploadCounter == null) return;
    if (gameUploadCounter === lastUploadSeen.current) return;
    lastUploadSeen.current = gameUploadCounter;
    if (gameUploadCounter === 0) return;
    if (!players || !s.homeAbbr || !s.awayAbbr) return;
    runSim();
  }, [gameUploadCounter, players, s.homeAbbr, s.awayAbbr, runSim]);

  // Legacy single-number scale kept for rollbackability, but real weight is gameEquivalents
  const seriesGameGoalMean = (() => {
    const unplayed = effG.filter(g=>!g.result);
    return unplayed.length>0 ? unplayed.reduce((a,g)=>a+(g.expTotal||BASELINE_GAME_GOALS),0)/unplayed.length : BASELINE_GAME_GOALS;
  })();
  const gameGoalScale = seriesGameGoalMean / BASELINE_GAME_GOALS;

  // Series Length — v31: sim probs when available; settled-impossibility per length.
  // A length L is impossible if: realized.gamesPlayed > L, OR if both teams' wins make L unreachable
  // (e.g., 5g requires 4-1 or 1-4 final; if hw=2 and aw=2 already, only 6g/7g possible).
  const lengthMkt = useMemo(()=>{
    const cf = [len4, len5, len6, len7];
    // v58: if sim is stale (inputs changed since last sim run), trust closed-form — otherwise use sim.
    //      Series length is one of the markets where closed-form is actually exact (no sim nuance needed),
    //      so falling back here is lossless. This makes the market respond to G4 winPct edits immediately,
    //      instead of needing a full 20k-trial sim re-run.
    const sim = simResult && simResult.seriesLengthProb;
    const useSimValues = sim && !simStale;
    const probs = [4,5,6,7].map((L,i) => useSimValues ? (sim[L]||0) : cf[i]);
    // settled detection: a length L is impossible if realized.gamesPlayed > L OR
    // if winner needs more wins than possible at length L given current (hw, aw)
    const settled = [4,5,6,7].map(L => {
      if (realized.gamesPlayed > L) return "no";
      // To finish in L games: winner has 4 wins after exactly L games.
      // The winner needs (4 - currentWins) more wins; loser at end has L-4 wins.
      // Both teams must satisfy: (4 - theirCurrentWins) games needed, and loser ends with (L-4).
      const homeNeedW = 4 - realized.hw, awayNeedW = 4 - realized.aw;
      const loserEndW = L - 4;
      // Home wins in L games: home wins (4-hw) of remaining, away ends with loserEndW (already at aw, can win loserEndW-aw more)
      const homeOK = homeNeedW >= 0 && homeNeedW <= realized.gamesRemaining && (loserEndW - realized.aw) >= 0 && (loserEndW - realized.aw) <= realized.gamesRemaining && (homeNeedW + (loserEndW - realized.aw)) === realized.gamesRemaining - (7 - L);
      const awayOK = awayNeedW >= 0 && awayNeedW <= realized.gamesRemaining && (loserEndW - realized.hw) >= 0 && (loserEndW - realized.hw) <= realized.gamesRemaining && (awayNeedW + (loserEndW - realized.hw)) === realized.gamesRemaining - (7 - L);
      // Simpler: just check if hw <= 4 && aw <= 4 && hw+aw <= L && (L-hw <= R OR L-aw <= R)
      // Even simpler heuristic: probs[i] from sim is 0 if impossible — trust it.
      return null;
    });
    // Use sim probs as authoritative impossibility signal: if sim prob == 0 AND we have realized games, it's settled NO.
    const lenAdj = probs.map((p, i) => {
      if (sim && p === 0 && realized.gamesPlayed > 0) return 0;
      return p;
    });
    const adjusted = applyMargin(lenAdj, effMargins.length);
    return {probs, lenAdj: adjusted, settled: lenAdj.map(p => p === 0 && realized.gamesPlayed > 0)};
  }, [effKey, effMargins.length, simResult, simStale, realized, len4, len5, len6, len7]);
  // Keep old lenAdj symbol for backward compat with the Length panel
  const lenAdjEffective = lengthMkt.lenAdj;

  const e8=useMemo(()=>{
    const src = (simResult && !simStale && simResult.exactScoreProb) ? simResult.exactScoreProb : outcomes;
    const rows=[{l:`${s.homeTeam||"Home"} 4-0`,k:"4-0"},{l:`${s.homeTeam||"Home"} 4-1`,k:"4-1"},{l:`${s.homeTeam||"Home"} 4-2`,k:"4-2"},{l:`${s.homeTeam||"Home"} 4-3`,k:"4-3"},{l:`${s.awayTeam||"Away"} 4-0`,k:"0-4"},{l:`${s.awayTeam||"Away"} 4-1`,k:"1-4"},{l:`${s.awayTeam||"Away"} 4-2`,k:"2-4"},{l:`${s.awayTeam||"Away"} 4-3`,k:"3-4"}].map(o=>({...o,tp:src[o.k]||0}));
    // v82: detect collapsing outcomes (single next-game event) AND apply per-outcome juice
    // for non-collapsing rows. No renormalization — preserves true probability mass when other
    // outcomes are removed (e.g. settled outcomes shouldn't inflate the surviving rows).
    // v84: removed the min(or-1, 0.12) cap. Full per-series margin now applies directly.
    const orE8 = effMargins.eightWay || 1.12;
    return rows.map(o => {
      const [hw, aw] = o.k.split("-").map(Number);
      let impossible;
      if (realized.seriesOver) {
        impossible = !(realized.hw === hw && realized.aw === aw);
      } else {
        impossible = realized.hw > hw || realized.aw > aw;
      }
      if (impossible) return {...o, _settled: true, tp: 0, ap: 0};
      const collapseSide = collapseSideForScore(hw, aw);
      let ap;
      if (collapseSide) {
        const ml = nextGameCollapsePrice(collapseSide);
        ap = ml != null ? ml : Math.min(0.995, o.tp * orE8);
      } else {
        ap = Math.min(0.995, o.tp * orE8);
      }
      return {...o, _settled: false, _collapseSide: collapseSide, _collapse: !!collapseSide, ap};
    });
  },[effKey,outcomes,effMargins.eightWay,effMargins.winner,s.homeTeam,s.awayTeam,simResult,simStale,realized,collapseSideForScore,nextGameCollapsePrice]);

  const winOrders=useMemo(()=>{
    const seqs=computeWinOrders(effG);
    const entries=Object.entries(seqs).map(([seq,tp])=>({seq,tp,hw:seq.split("").filter(c=>c==="H").length,aw:seq.split("").filter(c=>c==="A").length}));
    // v81: a sequence "collapses" to a single next-game event when its length equals
    // gamesPlayed+1 (i.e., this is the team clinching in the very next game).
    // Such rows price off the next-game moneyline (winPct × effMargins.winner), not series-market juice.
    // v82: do NOT renormalize non-collapsing rows after zeroing collapsing ones — that artificially
    // inflated the surviving series-market rows by a factor of 1/(1-pCollapsed). Instead, apply
    // per-outcome juice independently: each row gets `p × (1 + edge)`, capped at 0.995.
    // This makes each Win Order row's price stand on its own and stay consistent with broader
    // series markets (e.g., series-winner price when a team can only win in 7).
    // v84: removed the min(or-1, 0.06) cap. With per-series margins (v83) the user explicitly
    // sets the desired juice per series — capping it silently made the slider do nothing above 1.06.
    // Now the full margin applies: each row gets `p × OR`, capped at 0.995 to keep prices finite.
    const orWO = effMargins.winOrder || 1.06;
    return entries.map(e => {
      const collapses = e.seq.length === realized.gamesPlayed + 1 && !realized.seriesOver;
      const collapseSide = collapses ? (e.seq.charAt(e.seq.length-1)==="H" ? "home" : "away") : null;
      let ap;
      if (collapseSide) {
        const ml = nextGameCollapsePrice(collapseSide);
        ap = ml != null ? ml : Math.min(0.995, e.tp * orWO);
      } else {
        ap = Math.min(0.995, e.tp * orWO);
      }
      return {...e, _collapseSide: collapseSide, _collapse: !!collapseSide, ap};
    }).sort((a,b)=>b.ap-a.ap);
  },[effKey,effMargins.winOrder,effMargins.winner,realized,nextGameCollapsePrice]);

  // Score @ G3 — v31: relabel ("0-3" → "PHI 3-0"); detect settled when ≥3 games played.
  const cs3=useMemo(()=>{
    const st={};
    function rec(gi,hw,aw,prob){
      if(gi===3||hw===4||aw===4){const k=`${hw}-${aw}`;st[k]=(st[k]||0)+prob;return;}
      const g=effG[gi];
      if(g.result==="home")rec(gi+1,hw+1,aw,prob);
      else if(g.result==="away")rec(gi+1,hw,aw+1,prob);
      else{rec(gi+1,hw+1,aw,prob*g.winPct);rec(gi+1,hw,aw+1,prob*(1-g.winPct));}
    }
    rec(0,0,0,1);
    const homeAbbr = s.homeAbbr || "H";
    const awayAbbr = s.awayAbbr || "A";
    const labelFor = (k) => {
      const [hw, aw] = k.split("-").map(Number);
      if (hw > aw) return `${homeAbbr} ${hw}-${aw}`;
      if (aw > hw) return `${awayAbbr} ${aw}-${hw}`;
      return `Tied ${hw}-${aw}`;
    };
    const entries=Object.entries(st).map(([k,tp])=>{
      // settled YES: 3+ games already played AND this k matches realized state at G3
      const settled3Played = realized.gamesPlayed >= 3;
      const [hw, aw] = k.split("-").map(Number);
      // The realized H/A wins after EXACTLY 3 games
      let realizedHWat3 = 0, realizedAWat3 = 0;
      for (let i = 0; i < 3 && i < effG.length; i++) {
        if (effG[i].result === "home") realizedHWat3++;
        else if (effG[i].result === "away") realizedAWat3++;
      }
      const isThisOutcome = (hw === realizedHWat3 && aw === realizedAWat3);
      const settledYes = settled3Played && isThisOutcome;
      const settledNo = settled3Played && !isThisOutcome;
      let p = tp;
      if (settledYes) p = 1;
      if (settledNo) p = 0;
      return {k, label: labelFor(k), tp: p, _settled: settledYes || settledNo};
    });
    const adj=applyMargin(entries.map(e=>e.tp),effMargins.correctScore);
    return entries.map((e,i)=>({...e, ap: e._settled ? e.tp : adj[i]}))
      .sort((a,b)=>{
        if (!!a._settled !== !!b._settled) return a._settled ? 1 : -1;
        return b.tp - a.tp;
      });
  },[effKey,effMargins.correctScore,s.homeAbbr,s.awayAbbr,realized]);

  // Per-game OT market — v31: annotate settled (game already played) per game with realized OT result
  const otPerGame=useMemo(()=>effG.map((g,i)=>{
    const pOT=g.pOT??0.22;
    const [adjOT,adjNo]=applyMargin([pOT,1-pOT],effMargins.otGames);
    const settled = !!g.result;
    const wentOT = !!(g.wentOT || g.ot || g.result === "ot");
    return {game:i+1,pOT,adjOT,adjNo,expTotal:g.expTotal,winPct:g.winPct,_settled:settled,_wentOT:wentOT};
  }),[effKey,effMargins.otGames]);

  // Series OT games distribution — v31: sim PMF when available
  const otSeriesMkts=useMemo(()=>{
    let pmf;
    if (simResult && !simStale && simResult.seriesOTPMF) {
      pmf = simResult.seriesOTPMF;
    } else {
      // v87: closed-form PMF that respects realized OT count as a fixed offset.
      // Old code built Poisson(realized + future) which gave nonzero probability to k < realized
      // (impossible — you can't undo OT games already played). New code uses Poisson(future) shifted
      // by realized, so k < realized is exactly 0.
      const r = computeOTSeriesPMF(effG, 8);
      pmf = r.pmf;
    }
    let lambda=0; for (let k=0;k<pmf.length;k++) lambda += k*pmf[k];
    const exactLines=[0,1,2,3,4,5,6,7];
    // Settled exact: realized.otGames > k means "exactly k" is impossible (won't decrease).
    // realized.otGames == k AND no games remaining means "exactly k" is settled YES.
    const exactProbs = exactLines.map(k => pmf[k] || 0);
    const exactSettled = exactLines.map(k => {
      if (realized.otGames > k) return "no"; // already exceeded
      if (realized.otGames + realized.gamesRemaining < k) return "no"; // can't reach
      if (realized.gamesRemaining === 0 && realized.otGames === k) return "yes";
      return null;
    });
    const exactAdj = exactProbs.map((p, i) => {
      const st = exactSettled[i];
      if (st === "yes") return 1;
      if (st === "no") return 0;
      // Margin-adjust: just multiply this single-event probability by margin (1+margin/2 standard)
      return Math.min(1, p * effMargins.otExact);
    });
    const ouLines=[0.5,1.5,2.5,3.5];
    const ouRows = ouFromSimPMF(pmf, ouLines, effMargins.otGames, realized.otGames, realized.gamesRemaining);
    return {lambda, exactLines, exactProbs, exactAdj, exactSettled, ouLines, ouRows: sortSettled(ouRows)};
  },[effKey,effMargins.otExact,effMargins.otGames,simResult,realized]);

  // Spread market — v31: sim's exactScoreProb when available; per-row settled detection.
  // Lines are wins-spread (e.g., "-3.5" = win series by 4 wins, i.e., 4-0).
  // Spread market — v32: sim's exactScoreProb when available; settled detection by enumerating
  // all reachable final (hw, aw) outcomes from current realized state, then asking: do they all
  // cover (settled HOME) or none of them cover (settled AWAY)?
  const spreadMkt=useMemo(()=>{
    let outcomesObj;
    if (simResult && !simStale && simResult.exactScoreProb) {
      outcomesObj = simResult.exactScoreProb;
    } else {
      outcomesObj = outcomes;
    }
    const rows = computeSpread(outcomesObj, s.homeAbbr, s.awayAbbr);
    // Enumerate reachable finals from realized (rhw, raw, R remaining)
    const rhw = realized.hw, raw = realized.aw, R = realized.gamesRemaining;
    const reachable = [];
    if (rhw >= 4) reachable.push([rhw, raw]); // already over (shouldn't happen if pricer still active)
    else if (raw >= 4) reachable.push([rhw, raw]);
    else {
      // Home final = 4, away final ∈ [raw, min(3, raw+R-(4-rhw))]
      const homeAddNeeded = 4 - rhw;
      if (homeAddNeeded <= R) {
        for (let extraAway = 0; extraAway <= R - homeAddNeeded && raw + extraAway <= 3; extraAway++) {
          reachable.push([4, raw + extraAway]);
        }
      }
      // Away final = 4, home final ∈ [rhw, min(3, rhw+R-(4-raw))]
      const awayAddNeeded = 4 - raw;
      if (awayAddNeeded <= R) {
        for (let extraHome = 0; extraHome <= R - awayAddNeeded && rhw + extraHome <= 3; extraHome++) {
          reachable.push([rhw + extraHome, 4]);
        }
      }
    }
    // v49: cover logic must match the new spread convention.
    //   Home label "PIT -1.5" ⇒ home covers iff (hw - aw) > 1.5, i.e. hw - aw >= 2.
    //   Home label "PIT +1.5" ⇒ home covers iff (aw - hw) <= 1.5, i.e. hw - aw >= -1.
    //   Equivalent: home covers iff (hw - aw) > v where v is the NUMERIC part of the home label.
    //   For "PIT -1.5", v=-1.5 and (hw-aw) > -1.5 — WRONG. Need stronger: (hw-aw) >= 2.
    //   Cleaner: "home-favoured row" (r.line != null and r.line < 0) → covers iff diff >  |line|.
    //            "away-favoured row" (r.awayLine != null) → home covers iff diff >= -|awayLine| (i.e. diff > -|awayLine| - 1 equivalent for integer diffs; use >= for clarity).
    function homeCoversRow(row, hw, aw) {
      const diff = hw - aw;
      if (row.line != null && row.line < 0) return diff > Math.abs(row.line);   // home -X.5
      if (row.awayLine != null) return (aw - hw) <= Math.abs(row.awayLine);     // home +X.5 (equivalently: away doesn't cover -X.5)
      return null;
    }
    return rows.map(r=>{
      // Determine settled by checking all reachable finals
      let allCover = reachable.length > 0, noneCover = reachable.length > 0;
      for (const [h, a] of reachable) {
        const c = homeCoversRow(r, h, a);
        if (c) noneCover = false; else allCover = false;
      }
      const settledHome = allCover;
      const settledAway = noneCover;
      let pHome = r.pHome, pAway = r.pAway;
      if (settledHome) { pHome = 1; pAway = 0; }
      else if (settledAway) { pHome = 0; pAway = 1; }
      const [ah, aa] = (settledHome || settledAway) ? [pHome, pAway] : applyMargin([pHome, pAway], effMargins.spread);
      return {...r, pHome, pAway, ah, aa, _settled: settledHome || settledAway, _settledSide: settledHome ? "home" : settledAway ? "away" : null};
    }).sort((a,b)=>{
      if (!!a._settled !== !!b._settled) return a._settled ? 1 : -1;
      return 0;
    });
  },[effKey,effMargins.spread,outcomes,s.homeAbbr,s.awayAbbr,simResult,realized]);

  // Total goals O/U — v31: prefers sim PMF when available; settled rows handled
  const totalGoalsMkt=useMemo(()=>{
    let pmf;
    if (simResult && !simStale && simResult.seriesGoalsPMF) pmf = simResult.seriesGoalsPMF;
    else pmf = computeSeriesGoalsPMF(effG, 80);
    let lambda=0; for (let k=0;k<pmf.length;k++) lambda += k*pmf[k];
    // Build line ladder from where pAtLeast in [0.005, 0.995]
    let kMin=0, kMax=pmf.length-1;
    while (kMin<pmf.length && pAtLeast(pmf,kMin+1) >= 0.995) kMin++;
    while (kMax>0 && pAtLeast(pmf,kMax) <= 0.005) kMax--;
    const lines=[]; for (let k=Math.max(0,kMin); k<=kMax; k++) lines.push(k+0.5);
    // Realized total goals so far + max additional possible (assume 12 goals/game cap as soft sanity)
    const realizedTotal = realized.goalsH + realized.goalsA;
    const maxAdditional = realized.gamesRemaining * 12;
    const rows = ouFromSimPMF(pmf, lines, effMargins.totalGoals, realizedTotal, maxAdditional);
    return {lambda, lines: sortSettled(rows)};
  },[effKey,effMargins.totalGoals,simResult,realized]);

  // Shutouts O/U — v31: sim PMF when available; settled when realized count exceeds line
  const shutoutMkt=useMemo(()=>{
    let pmf;
    if (simResult && !simStale && simResult.seriesShutoutsPMF) pmf = simResult.seriesShutoutsPMF;
    else { const rate=s.shutoutRate??0.08; const multiplier = rate / 0.08; pmf=computeShutoutPMF(effG, multiplier, 8); }
    let lambda=0; for (let k=0;k<pmf.length;k++) lambda += k*pmf[k];
    const lines=[0.5,1.5,2.5,3.5];
    // Realized shutouts so far. Max additional = remaining games (each game can produce at most 1 shutout).
    const rows = ouFromSimPMF(pmf, lines, effMargins.shutouts, realized.shutouts, realized.gamesRemaining);
    // v88: exact shutout count market (0, 1, 2, 3, 4+).
    // pmf[k] is ALREADY P(total shutouts = k) — computeShutoutPMF uses degenerate PMFs for played
    // games, so realized count is baked in. v80's code wrongly shifted indices by realizedS, which
    // miscounted (e.g. with realizedS=1, P(total=1) was looking up pmf[0] instead of pmf[1]).
    //
    // Settled rules:
    //   k < realizedS  → impossible (can't undo realized shutouts)
    //   k > realizedS + remaining  → impossible (can't reach this many)
    //   else → tp = pmf[k]
    const realizedS = realized.shutouts || 0;
    const remaining = realized.gamesRemaining || 0;
    const exactBuckets = [];
    for (let k = 0; k < 4; k++) {
      const impossible = (k < realizedS) || (k > realizedS + remaining);
      const tp = impossible ? 0 : (k < pmf.length ? pmf[k] : 0);
      exactBuckets.push({label: String(k), tp, _settled: impossible, k});
    }
    // 4+ tail = sum of pmf[k] for k>=4, but only counts toward valid range.
    let tp4 = 0;
    for (let kk = 4; kk < pmf.length; kk++) tp4 += pmf[kk];
    const impossible4 = (realizedS + remaining) < 4;  // can't reach 4 even if all remaining games shut out
    if (realizedS >= 4) tp4 = 1; // already at 4+
    exactBuckets.push({label: "4+", tp: impossible4 ? 0 : tp4, _settled: impossible4, k: 4});
    const trueProbs = exactBuckets.map(r => r._settled ? 0 : r.tp);
    const adj = applyMargin(trueProbs, effMargins.shutouts);
    const exactRows = exactBuckets.map((r,i) => ({...r, ap: r._settled ? 0 : adj[i]}));
    return {lambda, lines: sortSettled(rows), exactRows};
  },[effKey,effMargins.shutouts,s.shutoutRate,simResult,realized]);

  // Team most goals — v31: sim convolution when available.
  // v79: closed-form fallback now uses computeTeamGoalsPMF (which respects realized scores)
  // instead of the legacy heuristic that only looked at series win prob (broken when one team
  // had a big realized goal lead, e.g. PHI 15-11 over PIT was being priced as PIT favored).
  const mostGoalsMkt=useMemo(()=>{
    let pHomeMost, pAwayMost, pTied;
    let H, A;
    if (simResult && !simStale && simResult.homeGoalsPMF && simResult.awayGoalsPMF) {
      H = simResult.homeGoalsPMF; A = simResult.awayGoalsPMF;
    } else {
      H = computeTeamGoalsPMF(effG, "home", 50);
      A = computeTeamGoalsPMF(effG, "away", 50);
    }
    let pH=0, pA=0, pT=0;
    for (let h=0; h<H.length; h++) {
      const ph = H[h]; if (!ph) continue;
      for (let a=0; a<A.length; a++) {
        const pa = A[a]; if (!pa) continue;
        const p = ph*pa;
        if (h>a) pH += p; else if (a>h) pA += p; else pT += p;
      }
    }
    pHomeMost = pH; pAwayMost = pA; pTied = pT;
    // Settled: home can't lose if (realizedH - realizedA) > maxRemaining*12
    const maxAdd = realized.gamesRemaining * 12;
    const homeLead = realized.goalsH - realized.goalsA;
    const settledHome = homeLead > maxAdd;
    const settledAway = -homeLead > maxAdd;
    if (settledHome) { pHomeMost = 1; pAwayMost = 0; pTied = 0; }
    else if (settledAway) { pHomeMost = 0; pAwayMost = 1; pTied = 0; }
    const [ah,aa] = (settledHome || settledAway) ? [pHomeMost, pAwayMost] : applyMargin([pHomeMost,pAwayMost],effMargins.teamMostGoals);
    return {pHomeMost,pAwayMost,pTied,ah,aa,_settled: settledHome || settledAway, _settledSide: settledHome ? "home" : settledAway ? "away" : null};
  },[effKey,effMargins.teamMostGoals,s.winnerGoalShift,hwp,awp,simResult,realized,effG]);

  // Per-team goals O/U — v31: sim PMFs when available
  const teamGoalsMkt=useMemo(()=>{
    let pmfH, pmfA;
    if (simResult && !simStale && simResult.homeGoalsPMF && simResult.awayGoalsPMF) {
      pmfH = simResult.homeGoalsPMF; pmfA = simResult.awayGoalsPMF;
    } else {
      pmfH = computeTeamGoalsPMF(effG,"home",50);
      pmfA = computeTeamGoalsPMF(effG,"away",50);
    }
    let lamH=0; for (let k=0;k<pmfH.length;k++) lamH += k*pmfH[k];
    let lamA=0; for (let k=0;k<pmfA.length;k++) lamA += k*pmfA[k];
    function ladder(pmf) {
      let kMin=0, kMax=pmf.length-1;
      while (kMin<pmf.length && pAtLeast(pmf,kMin+1) >= 0.995) kMin++;
      while (kMax>0 && pAtLeast(pmf,kMax) <= 0.005) kMax--;
      const lines=[]; for (let k=Math.max(0,kMin); k<=kMax; k++) lines.push(k+0.5);
      return lines;
    }
    const linesH=ladder(pmfH), linesA=ladder(pmfA);
    const maxAdd = realized.gamesRemaining * 12;
    return {
      home:{lambda:lamH, rows: sortSettled(ouFromSimPMF(pmfH, linesH, effMargins.teamGoals, realized.goalsH, maxAdd))},
      away:{lambda:lamA, rows: sortSettled(ouFromSimPMF(pmfA, linesA, effMargins.teamGoals, realized.goalsA, maxAdd))},
    };
  },[effKey,effMargins.teamGoals,simResult,realized]);

  // v31: realized state of this series — used to mark settled outcomes across all market panels.
  // Parlays — v49: G = next unplayed game × series winner, 4 combos
  const parlayMkt=useMemo(()=>{
    const {gameNum, rows} = computeParlays(effG, outcomes);
    const adj = applyMargin(rows.map(r=>r.tp), effMargins.parlay);
    return { gameNum, rows: rows.map((r,i)=>({...r, ap: adj[i]})) };
  },[effKey,effMargins.parlay,outcomes]);

  const MKTS=[
    {id:"winner",l:"Winner"},{id:"eightway",l:"Correct Score"},{id:"length",l:"Length"},
    {id:"spread",l:"Spread"},{id:"totalgoals",l:"Total Goals"},{id:"shutouts",l:"Shutouts"},
    {id:"winorder",l:"Win Order"},{id:"score3",l:"Score @G3"},
    {id:"ot",l:"OT/Game"},{id:"otseries",l:"OT Series"},{id:"otscorer",l:"OT Scorer"},
    {id:"firstotscorer",l:"First OT Scorer"},
    {id:"hattricks",l:"Hat Tricks"},
    {id:"mostgoals",l:"Most Goals"},{id:"teamgoals",l:"Team Goals"},
    {id:"parlay",l:"Parlay"},
    {id:"props",l:"O/U Props"},{id:"binary",l:"1+ Props"},
    {id:"propcombos",l:"Prop Combos"},
    {id:"goaliesaves",l:"Goalie Saves"},
    {id:"playerdetail",l:"Player Detail"},
    {id:"seriesleader",l:"Series Leader"},
  ];

  return (
    <div>
      {/* v38: Round selector — switch between R1/R2/R3/Final. Each round has its own series array. */}
      <div style={{display:"flex",gap:6,marginBottom:10,alignItems:"center"}}>
        <span style={{fontSize:10,fontWeight:500,letterSpacing:"0.1em",color:"var(--color-text-tertiary)",marginRight:6}}>ROUND</span>
        {ROUND_IDS.map(rid => (
          <button key={rid} onClick={()=>setCurrentRound&&setCurrentRound(rid)}
            style={{
              padding:"5px 14px",fontSize:11,fontWeight:500,borderRadius:"var(--border-radius-md)",cursor:"pointer",
              border:"0.5px solid",
              borderColor: currentRound===rid ? "#7c3aed" : "var(--color-border-secondary)",
              background: currentRound===rid ? "rgba(124,58,237,0.18)" : "var(--color-background-secondary)",
              color: currentRound===rid ? "#a78bfa" : "var(--color-text-secondary)",
            }}>
            {ROUND_LABELS[rid]}
          </button>
        ))}
      </div>
      <div style={{display:"flex",gap:6,marginBottom:14,flexWrap:"wrap",alignItems:"center"}}>
        {allSeries.map((sr,i)=><button key={i} onClick={()=>setSi(i)} style={{padding:"5px 11px",fontSize:11,borderRadius:"var(--border-radius-md)",border:"0.5px solid",cursor:"pointer",
          borderColor:safeSi===i?"#3b82f6":"var(--color-border-secondary)",background:safeSi===i?"#3b82f6":"var(--color-background-secondary)",color:safeSi===i?"white":"var(--color-text-secondary)"}}>
          {sr.homeAbbr&&sr.awayAbbr?`${sr.homeAbbr} v ${sr.awayAbbr}`:`${ROUND_LABELS[currentRound]||"S"} #${i+1}`}</button>)}
        <button onClick={()=>setShowMgn(v=>!v)} style={{marginLeft:"auto",padding:"4px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",cursor:"pointer",
          background:showMgn?"#1d4ed820":"var(--color-background-secondary)",border:showMgn?"0.5px solid #3b82f6":"0.5px solid var(--color-border-secondary)",
          color:showMgn?"#60a5fa":"var(--color-text-secondary)"}}>⚙ Margins</button>
        <button onClick={()=>setShowIRPanel(v=>!v)} style={{padding:"4px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",cursor:"pointer",
          background:showIRPanel?"#dc262620":"var(--color-background-secondary)",border:showIRPanel?"0.5px solid #ef4444":"0.5px solid var(--color-border-secondary)",
          color:showIRPanel?"#f87171":"var(--color-text-secondary)"}}>
          🚑 IR / Status{(()=>{const c=Object.keys(s.roleOverrides||{}).length;return c>0?` (${c})`:"";})()}
        </button>
      </div>

      {/* v60: Series status banner — shows current W-L state & next game context */}
      {s.homeAbbr && s.awayAbbr && (()=>{
        const hw = realized.hw, aw = realized.aw, gp = realized.gamesPlayed;
        const clinched = hw === 4 || aw === 4;
        let statusText, statusColor, nextContext = "";
        if (gp === 0) {
          statusText = `${s.homeAbbr} vs ${s.awayAbbr}`;
          statusColor = "var(--color-text-secondary)";
          nextContext = "Series not started";
        } else if (clinched) {
          const winnerAbbr = hw === 4 ? s.homeAbbr : s.awayAbbr;
          const winnerWins = Math.max(hw, aw), loserWins = Math.min(hw, aw);
          statusText = `${winnerAbbr} WINS SERIES ${winnerWins}-${loserWins}`;
          statusColor = "#22c55e";
          nextContext = "Series complete";
        } else if (hw === aw) {
          statusText = `Series tied ${hw}-${aw}`;
          statusColor = "#f59e0b";
          nextContext = `Game ${gp+1} next · ${HOME_PATTERN[gp+1]?s.homeAbbr:s.awayAbbr} hosts`;
        } else {
          const leaderAbbr = hw > aw ? s.homeAbbr : s.awayAbbr;
          statusText = `${leaderAbbr} leads series ${Math.max(hw,aw)}-${Math.min(hw,aw)}`;
          statusColor = hw > aw ? "#60a5fa" : "#f87171";
          nextContext = `Game ${gp+1} next · ${HOME_PATTERN[gp+1]?s.homeAbbr:s.awayAbbr} hosts`;
        }
        return <div style={{display:"flex",alignItems:"center",gap:14,padding:"10px 16px",marginBottom:14,
          background:`${statusColor}14`, border:`0.5px solid ${statusColor}44`, borderRadius:"var(--border-radius-md)",flexWrap:"wrap"}}>
          <div style={{fontSize:15,fontWeight:600,color:statusColor,letterSpacing:"0.02em"}}>{statusText}</div>
          <div style={{fontSize:11,color:"var(--color-text-tertiary)"}}>· {nextContext}</div>
          {gp > 0 && (
            <div style={{fontSize:11,color:"var(--color-text-tertiary)",marginLeft:"auto"}}>
              <span style={{color:"var(--color-text-secondary)"}}>{s.homeAbbr} {realized.goalsH}</span>
              <span style={{margin:"0 4px"}}>·</span>
              <span style={{color:"var(--color-text-secondary)"}}>{s.awayAbbr} {realized.goalsA}</span>
              <span style={{margin:"0 6px"}}>·</span>
              <span>Total: <span style={{color:"var(--color-text-secondary)",fontFamily:"var(--font-mono)"}}>{realized.goalsH + realized.goalsA}</span> ({((realized.goalsH+realized.goalsA)/gp).toFixed(2)}/g)</span>
              {realized.shutouts>0 && <span style={{margin:"0 6px"}}>·</span>}
              {realized.shutouts>0 && <span>Shutouts: {realized.shutouts}</span>}
              {realized.otGames>0 && <span style={{margin:"0 6px"}}>·</span>}
              {realized.otGames>0 && <span>OT: {realized.otGames}</span>}
            </div>
          )}
        </div>;
      })()}

      {showMgn&&<Card style={{marginBottom:14}}>
        <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8,flexWrap:"wrap"}}>
          <SH title="Market Margins" sub={`Overrides apply to ${s.homeAbbr||"this"} vs ${s.awayAbbr||"this"} ONLY. Settings tab edits affect all series.`}/>
          {s.marginOverrides && Object.keys(s.marginOverrides).length>0 && <>
            <span style={{fontSize:9,padding:"2px 6px",borderRadius:3,background:"rgba(245,158,11,0.15)",color:"#f59e0b",letterSpacing:0.3,fontWeight:500,marginLeft:"auto"}}>
              {Object.keys(s.marginOverrides).length} OVERRIDE{Object.keys(s.marginOverrides).length>1?"S":""}
            </span>
            <button onClick={()=>setAllSeries(p=>{const u=[...p];u[safeSi]={...u[safeSi],marginOverrides:undefined};return u;})}
              style={{padding:"3px 10px",fontSize:10,borderRadius:3,cursor:"pointer",background:"rgba(100,116,139,0.10)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)"}}>
              Reset All to Global
            </button>
          </>}
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(220px,1fr))",gap:8}}>
          {Object.entries(margins).map(([k,gv])=>{
            const isOverridden = s.marginOverrides && s.marginOverrides[k] != null;
            const v = isOverridden ? s.marginOverrides[k] : gv;
            return <label key={k} style={{fontSize:11,display:"flex",justifyContent:"space-between",alignItems:"center",gap:6}}>
              <span style={{color:"var(--color-text-secondary)",textTransform:"capitalize",display:"flex",alignItems:"center",gap:4}}>
                {k.replace(/([A-Z])/g," $1")}
                {isOverridden && <span style={{fontSize:8,padding:"1px 4px",borderRadius:2,background:"rgba(245,158,11,0.15)",color:"#f59e0b",letterSpacing:0.3,fontWeight:500}}>OVR</span>}
              </span>
              <div style={{display:"flex",alignItems:"center",gap:3}}>
                <LazyNI value={v} onCommit={nv=>updMarginOverride(k,nv)} min={1} max={3} step={0.01} style={{width:58,color:isOverridden?"#fbbf24":"var(--color-text-primary)"}}/>
                {isOverridden && <button onClick={()=>updMarginOverride(k, gv)} title="Reset to global" tabIndex={-1}
                  style={{padding:"1px 4px",fontSize:10,lineHeight:1,borderRadius:3,cursor:"pointer",background:"rgba(100,116,139,0.10)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)"}}>↻</button>}
              </div>
            </label>;
          })}
        </div>
      </Card>}

      {/* v92: per-series IR / status override panel.
          Lets user mark a player as IR or D2D for THIS series only. Global role unchanged.
          Useful when a player is healthy long-term but injured for the current series. */}
      {showIRPanel && s.homeAbbr && s.awayAbbr && <Card style={{marginBottom:14}}>
        <SH title="Per-Series Status Override" sub="Marks a player as IR/D2D for this series only. Global role unchanged."/>
        {(()=>{
          const teams = [s.homeAbbr, s.awayAbbr].filter(Boolean);
          const pool = (players||[])
            .filter(p => teams.includes(p.team))
            .filter(p => canonicalRole(p.lineRole) !== "CUT")
            .sort((a,b)=>a.team.localeCompare(b.team) || (b.pts||0) - (a.pts||0));
          const overrides = s.roleOverrides || {};
          const overrideKeys = new Set(Object.keys(overrides));
          // Show players with an override first, then the rest
          pool.sort((a,b)=>{
            const aOvr = overrideKeys.has(playerKey(a)) ? 0 : 1;
            const bOvr = overrideKeys.has(playerKey(b)) ? 0 : 1;
            if (aOvr !== bOvr) return aOvr - bOvr;
            return a.team.localeCompare(b.team) || (b.pts||0) - (a.pts||0);
          });
          function setOverride(p, value) {
            setAllSeries(prev => {
              const u = [...prev];
              const cur = u[safeSi] || {};
              const ovr = {...(cur.roleOverrides || {})};
              const k = playerKey(p);
              if (!value || value === "auto") delete ovr[k];
              else ovr[k] = value;
              u[safeSi] = {...cur, roleOverrides: Object.keys(ovr).length ? ovr : undefined};
              return u;
            });
          }
          const overrideCount = Object.keys(overrides).length;
          return <>
            <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:8,fontSize:11}}>
              {overrideCount > 0 && <>
                <span style={{color:"var(--color-text-secondary)"}}>{overrideCount} active override{overrideCount>1?"s":""}</span>
                <button onClick={()=>setAllSeries(p=>{const u=[...p];u[safeSi]={...u[safeSi],roleOverrides:undefined};return u;})}
                  style={{padding:"3px 8px",fontSize:10,borderRadius:3,background:"rgba(100,116,139,0.10)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>
                  Reset All
                </button>
              </>}
            </div>
            <div style={{maxHeight:300,overflowY:"auto",border:"0.5px solid var(--color-border-tertiary)",borderRadius:"var(--border-radius-md)"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <thead style={{position:"sticky",top:0,background:dark?"#131625":"#fff"}}>
                  <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                    <th style={{padding:"4px 8px",textAlign:"left",fontSize:9,color:"var(--color-text-tertiary)",fontWeight:500,letterSpacing:0.3}}>PLAYER</th>
                    <th style={{padding:"4px 8px",textAlign:"left",fontSize:9,color:"var(--color-text-tertiary)",fontWeight:500,letterSpacing:0.3}}>TEAM</th>
                    <th style={{padding:"4px 8px",textAlign:"left",fontSize:9,color:"var(--color-text-tertiary)",fontWeight:500,letterSpacing:0.3}}>BASE</th>
                    <th style={{padding:"4px 8px",textAlign:"left",fontSize:9,color:"var(--color-text-tertiary)",fontWeight:500,letterSpacing:0.3}}>SERIES STATUS</th>
                  </tr>
                </thead>
                <tbody>{pool.slice(0,200).map((p,i)=>{
                  const k = playerKey(p);
                  const ovrVal = overrides[k] || "";
                  const baseRole = canonicalRole(p.lineRole) || "—";
                  return <tr key={k} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:ovrVal?(dark?"rgba(220,38,38,0.06)":"rgba(220,38,38,0.04)"):(i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"))}}>
                    <td style={{padding:"3px 8px"}}>{p.name}</td>
                    <td style={{padding:"3px 8px"}}><span style={{fontSize:9,padding:"1px 5px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
                    <td style={{padding:"3px 8px"}}><RoleBadge role={baseRole}/></td>
                    <td style={{padding:"3px 8px"}}>
                      <select value={ovrVal} onChange={e=>setOverride(p, e.target.value)}
                        style={{padding:"2px 6px",fontSize:11,background:"var(--color-background-secondary)",border:`0.5px solid ${ovrVal?"#dc2626":"var(--color-border-secondary)"}`,borderRadius:3,color:ovrVal?"#f87171":"var(--color-text-primary)"}}>
                        <option value="">Auto (use base)</option>
                        <option value="IR">IR (out for series)</option>
                        <option value="D2D">D2D (miss next game)</option>
                        <option value="ACTIVE">ACTIVE (healthy scratch)</option>
                      </select>
                    </td>
                  </tr>;
                })}</tbody>
              </table>
            </div>
          </>;
        })()}
      </Card>}

      {s.homeAbbr && s.awayAbbr && <Card style={{marginBottom:10,background:simStale?"rgba(245,158,11,0.04)":"rgba(124,58,237,0.04)",border:`0.5px solid ${simStale?"rgba(245,158,11,0.25)":"rgba(124,58,237,0.25)"}`}}>
        <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:simResult?8:0,flexWrap:"wrap"}}>
          <SH title="Unified Sim (MC 20k)" sub={simResult?`L1 correlation · ${simResult.simMs}ms · ${simResult.pool.length} skaters${simStale?" · STALE — inputs changed, re-run to refresh":""}`:"Click Run to simulate. Independent of closed-form prices above."}/>
          {simResult && !simStale && <span style={{fontSize:9,padding:"2px 6px",borderRadius:3,background:"rgba(124,58,237,0.15)",color:"#a78bfa",letterSpacing:0.4,fontWeight:500}}>DUAL-TRACK</span>}
          {simStale && <span style={{fontSize:9,padding:"2px 6px",borderRadius:3,background:"rgba(245,158,11,0.15)",color:"#f59e0b",letterSpacing:0.4,fontWeight:500}}>STALE</span>}
          <button onClick={runSim} disabled={simRunning||!players||!!runAllProgress} style={{marginLeft:"auto",padding:"4px 12px",fontSize:11,fontWeight:500,borderRadius:4,cursor:simRunning?"wait":"pointer",background:simRunning?"var(--color-background-secondary)":simStale?"#f59e0b":"#7c3aed",color:simRunning?"var(--color-text-tertiary)":"white",border:"none"}}>
            {simRunning?"Running…":simResult?(simStale?"Re-run":"Re-run"):"Run Unified Sim"}
          </button>
          <button onClick={runAllSims} disabled={!players||simRunning||!!runAllProgress}
            title="Run unified sim for every series in this round (sequentially, ~6s per series)"
            style={{padding:"4px 12px",fontSize:11,fontWeight:500,borderRadius:4,cursor:runAllProgress?"wait":"pointer",
              background: runAllProgress ? "var(--color-background-secondary)" : "#10b981",
              color: runAllProgress ? "var(--color-text-tertiary)" : "white", border:"none"}}>
            {runAllProgress ? `Running ${runAllProgress.current}/${runAllProgress.total}…` : "Run All Sims"}
          </button>
        </div>
        {simResult && <>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))",gap:10,fontSize:11}}>
          <div>
            <div style={{fontSize:9,color:"var(--color-text-tertiary)",letterSpacing:0.4,textTransform:"uppercase",marginBottom:3}}>Winner</div>
            <div style={{fontFamily:"var(--font-mono)",fontSize:12}}>
              <div>{s.homeAbbr||"H"}: <span style={{color:"#60a5fa"}}>{(simResult.winnerProb.H*100).toFixed(1)}%</span> <span style={{color:"var(--color-text-tertiary)",fontSize:10}}>(CF: {(hwp*100).toFixed(1)}% · Δ {((simResult.winnerProb.H-hwp)*100).toFixed(2)})</span></div>
              <div>{s.awayAbbr||"A"}: <span style={{color:"#60a5fa"}}>{(simResult.winnerProb.A*100).toFixed(1)}%</span> <span style={{color:"var(--color-text-tertiary)",fontSize:10}}>(CF: {(awp*100).toFixed(1)}% · Δ {((simResult.winnerProb.A-awp)*100).toFixed(2)})</span></div>
            </div>
          </div>
          <div>
            <div style={{fontSize:9,color:"var(--color-text-tertiary)",letterSpacing:0.4,textTransform:"uppercase",marginBottom:3}}>Series Length</div>
            <div style={{fontFamily:"var(--font-mono)",fontSize:11}}>
              {[4,5,6,7].map(n=>{
                const cfLen=n===4?((outcomes["4-0"]||0)+(outcomes["0-4"]||0)):n===5?((outcomes["4-1"]||0)+(outcomes["1-4"]||0)):n===6?((outcomes["4-2"]||0)+(outcomes["2-4"]||0)):((outcomes["4-3"]||0)+(outcomes["3-4"]||0));
                const mc=simResult.seriesLengthProb[n];
                return <div key={n}>{n}g: <span style={{color:"#60a5fa"}}>{((mc||0)*100).toFixed(1)}%</span> <span style={{color:"var(--color-text-tertiary)",fontSize:10}}>(CF: {(cfLen*100).toFixed(1)}%)</span></div>;
              })}
            </div>
          </div>
          <div>
            <div style={{fontSize:9,color:"var(--color-text-tertiary)",letterSpacing:0.4,textTransform:"uppercase",marginBottom:3}}>Series Goals</div>
            <div style={{fontFamily:"var(--font-mono)",fontSize:11}}>
              {(()=>{
                let mean=0; for(let k=0;k<simResult.seriesGoalsPMF.length;k++) mean+=k*simResult.seriesGoalsPMF[k];
                return <>
                  <div>Mean: <span style={{color:"#60a5fa"}}>{mean.toFixed(2)}</span> <span style={{color:"var(--color-text-tertiary)",fontSize:10}}>(CF: {totalGoalsMkt.lambda.toFixed(2)})</span></div>
                  <div>Shutouts: <span style={{color:"#60a5fa"}}>{simResult.avgShutouts.toFixed(2)}</span> <span style={{color:"var(--color-text-tertiary)",fontSize:10}}>(CF: {shutoutMkt.lambda.toFixed(2)})</span></div>
                  <div>OT games: <span style={{color:"#60a5fa"}}>{simResult.avgOT.toFixed(2)}</span></div>
                </>;
              })()}
            </div>
          </div>
        </div>
        <div style={{marginTop:6,fontSize:9,color:"var(--color-text-tertiary)"}}>
          CF = closed-form (current production prices). Δ = MC − CF. Expect small non-zero deltas from sampling noise (~0.2–0.3% at 20k). Consistent bias &gt; 1% = signal.
        </div>
        </>}
      </Card>}

      <div style={{display:"grid",gridTemplateColumns:"380px minmax(0,1fr)",gap:18,alignItems:"start"}}>
        <div>
          <Card style={{marginBottom:10}}>
            <SH title="Series Setup"/>
            <div style={{display:"grid",gridTemplateColumns:"1fr 66px",gap:4,marginBottom:8}}>
              {[["homeTeam","Home team"],["homeAbbr","Abbr"],["awayTeam","Away team"],["awayAbbr","Abbr"]].map(([f,ph])=>(
                <LazyText key={f} placeholder={ph} value={s[f]||""}
                  onCommit={v=>updS(f,v)}
                  transform={f.includes("Abbr") ? (x=>x.toUpperCase()) : null}
                  style={{padding:"4px 7px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:4,color:"var(--color-text-primary)"}}/>
              ))}
            </div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11,tableLayout:"fixed"}}>
              <colgroup>
                <col style={{width:"22px"}}/>
                <col style={{width:"34px"}}/>
                <col style={{width:"50px"}}/>
                <col style={{width:"42px"}}/>
                <col style={{width:"40px"}}/>
                <col style={{width:"70px"}}/>
                <col style={{width:"60px"}}/>
              </colgroup>
              <thead><tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                {[
                  ["G",""],
                  ["Host","Which team hosts this game (2-2-1-1-1 rotation based on series home team)"],
                  ["","Series HOME team's win probability for this game (ALWAYS from the series-home-team's perspective, regardless of who hosts). For games hosted by the away team, this should typically be lower to reflect home-ice advantage."],
                  ["Dec","Decimal odds for series HOME team to win this game. Editable — changes here update Win% automatically. Dec = 1 / Win%."],
                  ["Total","Expected total goals for this game"],
                  ["OT%","P(game goes to OT). NHL playoff avg ~22%. Affects OT markets only, not series outcome."],
                  ["Score",""],
                  ["Result",""]
                ].map(([h,tip],idx)=>{
                  const label = idx===2 ? ((s.homeAbbr||"H")+" Win%") : h;
                  return <th key={idx} style={{padding:"3px 3px",color:"var(--color-text-tertiary)",fontWeight:500,textAlign:"left",fontSize:9,cursor:tip?"help":"default"}} title={tip||undefined}>{label}</th>;
                })}
              </tr></thead>
              <tbody>{effG.map((g,i)=>{
                const isHome=HOME_PATTERN[i+1];
                const homeLabel=isHome?(s.homeAbbr||"H"):(s.awayAbbr||"A");
                return (
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:g.result?0.5:1}}>
                  <td style={{padding:"2px 3px",color:"var(--color-text-tertiary)",fontSize:9}}>G{i+1}</td>
                  <td style={{padding:"2px 3px",fontSize:9,color:"var(--color-text-secondary)"}}>{homeLabel}</td>
                  <td style={{padding:"1px 2px"}}><LazyNI value={+g.winPct.toFixed(3)} onCommit={v=>updG(i,"winPct",v)} min={0} max={1} step={0.01} style={{width:46}} showSpinner={false}/></td>
                  <td style={{padding:"1px 2px"}}>
                    <LazyNI value={+(1/Math.max(0.01,g.winPct)).toFixed(2)} onCommit={v=>{
                      // v60: user enters decimal odds for series HOME team → convert back to winPct
                      const dec = Math.max(1.01, Math.min(100, v));
                      updG(i, "winPct", +(1/dec).toFixed(4));
                    }} min={1.01} max={100} step={0.05} style={{width:50}} showSpinner={false}/>
                  </td>
                  <td style={{padding:"1px 2px"}}><LazyNI value={+g.expTotal.toFixed(1)} onCommit={v=>updG(i,"expTotal",v)} min={0.5} max={12} step={0.1} style={{width:40}} showSpinner={false}/></td>
                  <td style={{padding:"1px 2px",position:"relative"}}>
                    <LazyNI value={+(g.pOT??0.22).toFixed(2)} onCommit={v=>updG(i,"pOT",v)} min={0} max={0.5} step={0.01} style={{width:38}} showSpinner={false}/>
                    {s.games[i]?.pOT_manual && <span title="Manually overridden — click Auto OT% to reset" style={{position:"absolute",top:0,right:-2,fontSize:8,color:"#f59e0b"}}>*</span>}
                  </td>
                  <td style={{padding:"1px 2px"}}>
                    <div style={{display:"flex",gap:2,alignItems:"center"}}>
                      <input type="number" min={0} max={20} value={g.homeScore??""} placeholder="—"
                        onChange={e=>updG(i,"homeScore",parseInt(e.target.value)||null)}
                        style={{width:28,fontSize:9,textAlign:"center",padding:"2px 2px",fontFamily:"var(--font-mono)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}/>
                      <span style={{fontSize:9,color:"var(--color-text-tertiary)"}}>-</span>
                      <input type="number" min={0} max={20} value={g.awayScore??""} placeholder="—"
                        onChange={e=>updG(i,"awayScore",parseInt(e.target.value)||null)}
                        style={{width:28,fontSize:9,textAlign:"center",padding:"2px 2px",fontFamily:"var(--font-mono)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}/>
                    </div>
                  </td>
                  <td style={{padding:"1px 3px"}}>
                    <select value={g.result||""} onChange={e=>updG(i,"result",e.target.value||null)}
                      style={{fontSize:9,padding:"2px 3px",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)",width:48}}>
                      <option value="">—</option>
                      <option value="home">{s.homeAbbr||"Home"} W</option>
                      <option value="away">{s.awayAbbr||"Away"} W</option>
                    </select>
                  </td>
                </tr>
                );
              })}</tbody>
            </table>
            <div style={{marginTop:8,paddingTop:8,borderTop:"0.5px solid var(--color-border-tertiary)"}}>
              <div style={{display:"flex",gap:10,fontSize:10,alignItems:"center",flexWrap:"wrap"}}>
                <label style={{color:"var(--color-text-secondary)",display:"flex",gap:4,alignItems:"center"}}>
                  Shutout/G: <LazyNI value={s.shutoutRate??0.08} onCommit={v=>updS("shutoutRate",v)} min={0} max={0.5} step={0.01} style={{width:44}} showSpinner={false}/>
                </label>
                <label style={{color:"var(--color-text-secondary)",display:"flex",gap:4,alignItems:"center"}}>
                  Goal shift: <LazyNI value={s.winnerGoalShift??0.15} onCommit={v=>updS("winnerGoalShift",v)} min={0} max={0.4} step={0.01} style={{width:44}} showSpinner={false}/>
                </label>
                {/* v62: Realized-scoring adjustment toggle */}
                <label style={{color:"var(--color-text-secondary)",display:"flex",gap:4,alignItems:"center"}} title="Adjusts future player goal lambdas based on realized scoring vs expected so far. Helps match book lines when goalies are hot/cold or scoring is way off baseline.">
                  Realized adj:
                  <select value={s.realizedAdjMode||"off"} onChange={e=>updS("realizedAdjMode",e.target.value)}
                    style={{fontSize:10,padding:"2px 4px",borderRadius:3,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-primary)"}}>
                    <option value="off">Off</option>
                    <option value="combined">Combined</option>
                    <option value="perTeam">Per-team</option>
                  </select>
                </label>
                {realizedAdjMode!=="off" && realizedAdj.gp>0 && (
                  <span style={{fontSize:9,color:"var(--color-text-tertiary)",fontFamily:"var(--font-mono)"}}>
                    {realizedAdjMode==="combined" ? `×${realizedAdj.combined.toFixed(2)}` :
                      `${s.homeAbbr||"H"}×${realizedAdj.home.toFixed(2)} ${s.awayAbbr||"A"}×${realizedAdj.away.toFixed(2)}`}
                  </span>
                )}
                <button onClick={()=>setAllSeries(p=>{const u=[...p];u[safeSi]={...u[safeSi],games:u[safeSi].games.map(g=>({...g,pOT_manual:false,pOT:null}))};return u;})}
                  style={{fontSize:9,padding:"3px 8px",background:"transparent",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-secondary)",cursor:"pointer"}}
                  title="Reset all games' OT% to auto-computed values from expTotal and winPct">
                  Auto OT%
                </button>
                {autoWinPct!=null && <button onClick={()=>{
                  // Seed Game 1 with xG-derived win% from team strength; pattern propagates to other games via updG logic
                  setAllSeries(p=>{
                    const u=[...p];
                    const games=[...u[safeSi].games];
                    games[0]={...games[0],winPct:+autoWinPct.toFixed(3)};
                    for(let i=1;i<7;i++){
                      games[i]={...games[i],winPct:HOME_PATTERN[i+1]?+autoWinPct.toFixed(3):+(1-autoWinPct).toFixed(3)};
                    }
                    u[safeSi]={...u[safeSi],games};
                    return u;
                  });
                }}
                  style={{fontSize:9,padding:"3px 8px",background:"rgba(59,130,246,0.12)",border:"0.5px solid #3b82f6",borderRadius:3,color:"#60a5fa",cursor:"pointer"}}
                  title={`Derive Game 1 Win% from team xG differential. ${s.homeAbbr} diff60: ${homeStrength?.diff60.toFixed(3)}, ${s.awayAbbr} diff60: ${awayStrength?.diff60.toFixed(3)}. Auto = ${(autoWinPct*100).toFixed(1)}%`}>
                  Auto Win% ({(autoWinPct*100).toFixed(0)}%)
                </button>}
                <span style={{marginLeft:"auto",color:"var(--color-text-tertiary)"}}>Exp {expG.toFixed(2)}g</span>
              </div>
              <div style={{marginTop:4,fontSize:9,color:"var(--color-text-tertiary)"}}>H:{s.games.filter(g=>g.result==="home").length} A:{s.games.filter(g=>g.result==="away").length} · TtlGoals λ {totalGoalsMkt.lambda.toFixed(1)} · Shut λ {shutoutMkt.lambda.toFixed(2)}</div>
              {(homeStrength||awayStrength) && <div style={{marginTop:6,padding:"5px 7px",background:"var(--color-background-secondary)",borderRadius:3,fontSize:9,color:"var(--color-text-tertiary)",fontFamily:"var(--font-mono)"}}>
                Team Strength (xG/60): {homeStrength ? `${s.homeAbbr} F=${homeStrength.xGF60.toFixed(2)} A=${homeStrength.xGA60.toFixed(2)} Δ=${homeStrength.diff60>=0?"+":""}${homeStrength.diff60.toFixed(3)}` : `${s.homeAbbr||"?"} —`} · {awayStrength ? `${s.awayAbbr} F=${awayStrength.xGF60.toFixed(2)} A=${awayStrength.xGA60.toFixed(2)} Δ=${awayStrength.diff60>=0?"+":""}${awayStrength.diff60.toFixed(3)}` : `${s.awayAbbr||"?"} —`}
              </div>}
            </div>
          </Card>

          {/* v92: Per-game goalie assignment (replaces v23 design).
              Default ("Auto") = first goalie with STARTER role, else highest starter_share.
              Goalies with 0 projected games (BACKUP w/ 0 share, IR, CUT) are hidden from dropdowns. */}
          {goalies && s.homeAbbr && s.awayAbbr && <Card style={{marginBottom:10}}>
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6}}>
              <SH title="Per-Game Goalie" sub="Defaults to STARTER role · used for opposing-team goal lambda + saves market"/>
              <button onClick={()=>setAllSeries(p=>{const u=[...p];u[safeSi]={...u[safeSi],games:u[safeSi].games.map(g=>({...g,homeGoalie:null,awayGoalie:null}))};return u;})}
                style={{marginLeft:"auto",fontSize:9,padding:"3px 8px",background:"transparent",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-secondary)",cursor:"pointer"}}
                title="Clear all per-game goalie overrides; revert to STARTER">
                Reset All to Auto
              </button>
            </div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
              <thead><tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                <th style={{padding:"3px 2px",textAlign:"left",color:"var(--color-text-tertiary)",fontWeight:500,fontSize:9}}>G</th>
                <th style={{padding:"3px 2px",textAlign:"left",color:"var(--color-text-tertiary)",fontWeight:500,fontSize:9}}>{s.homeAbbr||"H"} G</th>
                <th style={{padding:"3px 2px",textAlign:"left",color:"var(--color-text-tertiary)",fontWeight:500,fontSize:9}}>{s.awayAbbr||"A"} G</th>
              </tr></thead>
              <tbody>{[0,1,2,3,4,5,6].map(i=>{
                const g = s.games[i];
                // v92: filter goalies projected for ≥1 series game.
                // Projection: STARTER (full series share), BACKUP w/ starter_share>=0.05 (occasional fill-in).
                // Hidden: IR, CUT, BACKUPS with starter_share<0.05.
                const projects = (gg) => {
                  const r = canonicalRole(gg.lineRole);
                  if (r === "IR" || r === "CUT") return false;
                  if (r === "STARTER") return true;
                  if (r === "BACKUP") return (gg.starter_share || 0) >= 0.05;
                  // No role set — fall back to starter_share threshold
                  return (gg.starter_share || 0) >= 0.05;
                };
                const homeG = goalies.filter(gg=>gg.team===s.homeAbbr && projects(gg));
                const awayG = goalies.filter(gg=>gg.team===s.awayAbbr && projects(gg));
                // Auto-pick: STARTER first, else highest starter_share
                const pickAuto = (pool) => {
                  const starter = pool.find(gg => canonicalRole(gg.lineRole) === "STARTER");
                  if (starter) return starter;
                  return pool.reduce((b,gg)=>(!b||gg.starter_share>b.starter_share)?gg:b, null);
                };
                const autoHomeG = pickAuto(homeG);
                const autoAwayG = pickAuto(awayG);
                const selHome = g.homeGoalie || (autoHomeG?.name||"");
                const selAway = g.awayGoalie || (autoAwayG?.name||"");
                const inp = {width:"100%",padding:"2px 3px",fontSize:9,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"};
                return <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:g.result?0.5:1}}>
                  <td style={{padding:"1px 2px",fontSize:9,color:"var(--color-text-tertiary)"}}>G{i+1}</td>
                  <td style={{padding:"1px 2px"}}>
                    <select value={selHome} onChange={e=>updG(i,"homeGoalie",e.target.value||null)} style={inp}>
                      {homeG.map(gg=>(<option key={gg.name} value={gg.name}>{gg.name.split(" ").pop()} ({(gg.quality??1).toFixed(2)})</option>))}
                    </select>
                    {g.homeGoalie && <span title="manually overridden" style={{fontSize:8,color:"#f59e0b"}}>*</span>}
                  </td>
                  <td style={{padding:"1px 2px"}}>
                    <select value={selAway} onChange={e=>updG(i,"awayGoalie",e.target.value||null)} style={inp}>
                      {awayG.map(gg=>(<option key={gg.name} value={gg.name}>{gg.name.split(" ").pop()} ({(gg.quality??1).toFixed(2)})</option>))}
                    </select>
                    {g.awayGoalie && <span title="manually overridden" style={{fontSize:8,color:"#f59e0b"}}>*</span>}
                  </td>
                </tr>;
              })}</tbody>
            </table>
            <div style={{marginTop:4,fontSize:8,color:"var(--color-text-tertiary)",fontStyle:"italic"}}>Quality &gt;1.0 = elite (suppresses opposing scorers). Per-game lambdas auto-adjust by 1/quality of opposing goalie.</div>
          </Card>}
          <Card>
            <SH title="Quick Summary"/>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,marginBottom:6}}>
              {[[s.homeTeam||"Home",adjH],[s.awayTeam||"Away",adjA]].map(([n,ap],i)=>(
                <div key={i} style={{padding:"7px 8px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",textAlign:"center"}}>
                  <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:2}}>{n}</div>
                  <div style={{fontFamily:"var(--font-mono)",fontSize:15,fontWeight:500,color:ap>=0.5?"#4ade80":"var(--color-text-primary)"}}>{fmt(ap)}</div>
                  <div style={{fontSize:9,color:"var(--color-text-tertiary)"}}>{(ap*100).toFixed(1)}%</div>
                </div>
              ))}
            </div>
            <div style={{display:"flex",gap:4,marginBottom:8}}>
              {[["4g",lenAdjEffective[0]],["5g",lenAdjEffective[1]],["6g",lenAdjEffective[2]],["7g",lenAdjEffective[3]]].map(([l,ap])=>(
                <div key={l} style={{flex:1,padding:"4px 5px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",textAlign:"center"}}>
                  <div style={{fontSize:9,color:"var(--color-text-tertiary)"}}>{l}</div>
                  <div style={{fontFamily:"var(--font-mono)",fontSize:10,fontWeight:500}}>{fmt(ap)}</div>
                </div>
              ))}
            </div>
            {onEnterGame&&(()=>{
              // Find next unplayed game
              const nextGame=s.games.findIndex(g=>!g.result);
              const label=nextGame===-1?"All games entered":`Enter G${nextGame+1} Result`;
              return <button
                disabled={nextGame===-1||(!s.homeAbbr&&!s.awayAbbr)}
                onClick={()=>onEnterGame({seriesIdx:si,gameIdx:nextGame})}
                style={{width:"100%",padding:"7px 0",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",
                  border:"none",cursor:nextGame===-1?"default":"pointer",
                  background:nextGame===-1?"var(--color-background-secondary)":"#10b981",
                  color:nextGame===-1?"var(--color-text-tertiary)":"white"}}>
                {label}
              </button>;
            })()}
          </Card>
        </div>

        <div>
          <div style={{display:"flex",gap:0,marginBottom:12,borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)",width:"fit-content",flexWrap:"wrap"}}>
            {MKTS.map(m=><button key={m.id} onClick={()=>setMkt(m.id)} style={{padding:"5px 12px",fontSize:11,border:"none",borderRight:"0.5px solid var(--color-border-tertiary)",cursor:"pointer",
              background:mkt===m.id?"#1d4ed8":"var(--color-background-secondary)",color:mkt===m.id?"white":"var(--color-text-secondary)"}}>{m.l}</button>)}
          </div>

          {/* v64: series-over banner — most series markets are no longer tradeable once a team has 4 wins */}
          {realized.seriesOver && mkt!=="props" && mkt!=="binary" && mkt!=="goaliesaves" && mkt!=="playerdetail" && mkt!=="seriesleader" && (
            <div style={{padding:"10px 14px",marginBottom:12,background:"rgba(34,197,94,0.10)",border:"0.5px solid rgba(34,197,94,0.3)",borderRadius:"var(--border-radius-md)",fontSize:12,color:"#4ade80",fontWeight:500}}>
              ✓ SERIES COMPLETE — {realized.hw>=4 ? (s.homeTeam||s.homeAbbr||"Home") : (s.awayTeam||s.awayAbbr||"Away")} won {Math.max(realized.hw,realized.aw)}-{Math.min(realized.hw,realized.aw)}. Series-level markets are settled; player props remain available.
            </div>
          )}

          {mkt==="winner"&&<Card><SH title="Series Winner" sub={`OR: ${effMargins.winner}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Team",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody><OR label={s.homeTeam||"Home"} tp={hwp} ap={adjH} showTrue={showTrue}/><OR label={s.awayTeam||"Away"} tp={awp} ap={adjA} showTrue={showTrue}/></tbody></table>
          </Card>}

          {mkt==="eightway"&&<Card><SH title="Series Correct Score" sub={`OR: ${effMargins.eightWay}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Outcome",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody>{e8.map((o,i)=>(
              <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:o._settled?0.4:1,textDecoration:o._settled?"line-through":"none"}}>
                <td style={{padding:"5px 8px"}}>{o.l}{o._settled&&<span style={{marginLeft:6,fontSize:9,color:"#ef4444",textDecoration:"none",display:"inline-block"}}>impossible</span>}{o._collapse&&<span style={{marginLeft:6,fontSize:9,padding:"1px 5px",borderRadius:2,background:"rgba(34,197,94,0.15)",color:"#4ade80",letterSpacing:0.3,fontWeight:500}} title="Priced at next-game moneyline (single-game collapse)">ML</span>}</td>
                {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(o.tp*100).toFixed(1)}%</td>}
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.ap*100).toFixed(1)}%</td>
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:o.ap>=0.5?"#4ade80":"var(--color-text-primary)"}}>{o._settled?"—":fmt(o.ap)}</td>
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{o._settled?"—":toDec(o.ap).toFixed(2)}</td>
              </tr>
            ))}</tbody></table>
          </Card>}

          {mkt==="length"&&<Card><SH title="Series Length" sub={`Realized: ${realized.gamesPlayed}g played · OR: ${effMargins.length}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Games",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody>{(()=>{
              const data = [["4 Games",4,len4,lenAdjEffective[0]],["5 Games",5,len5,lenAdjEffective[1]],["6 Games",6,len6,lenAdjEffective[2]],["7 Games",7,len7,lenAdjEffective[3]]];
              const annotated = data.map(([l,L,tp,ap],i) => ({l,L,tp,ap,_settled: lengthMkt.settled[i] || (realized.gamesPlayed > L)}));
              annotated.sort((a,b)=>{ if (!!a._settled !== !!b._settled) return a._settled ? 1 : -1; return 0; });
              return annotated.map((o,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:o._settled?0.4:1}}>
                  <td style={{padding:"5px 8px"}}>{o.l}{o._settled&&<span style={{marginLeft:6,fontSize:9,color:"#ef4444"}}>impossible</span>}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(o.tp*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.ap*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:o.ap>=0.5?"#4ade80":"var(--color-text-primary)"}}>{o._settled?"—":fmt(o.ap)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{o._settled?"—":toDec(o.ap).toFixed(2)}</td>
                </tr>
              ));
            })()}</tbody></table>
            <div style={{marginTop:8,padding:"5px 8px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",fontSize:11,color:"var(--color-text-secondary)"}}>
              Exp length: <strong style={{color:"var(--color-text-primary)"}}>{expG.toFixed(2)}g</strong></div>
            {/* v49: O/U series length */}
            <div style={{marginTop:12}}>
              <SH title="Length O/U" sub={`OR: ${effMargins.length}x`}/>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <TH cols={["Line",...(showTrue?["True O%","True U%"]:[]),"O Adj%","U Adj%","Over","Under"]}/>
                <tbody>{(()=>{
                  // v86: use closed-form when sim is stale (matches the top "Series Length" section logic).
                  // Previously used stale sim probs, which gave wrong O/U numbers after input changes.
                  // Cumulative prob of going > line:
                  //   P(over 4.5) = P(len >= 5) = len5 + len6 + len7
                  //   P(over 5.5) = len6 + len7
                  //   P(over 6.5) = len7
                  const cf = [len4, len5, len6, len7];
                  const sim = simResult && simResult.seriesLengthProb;
                  const useSim = sim && !simStale;
                  const srcLens = useSim ? [sim[4]||0, sim[5]||0, sim[6]||0, sim[7]||0] : cf;
                  const lines = [
                    {line:4.5, pO: srcLens[1]+srcLens[2]+srcLens[3]},
                    {line:5.5, pO: srcLens[2]+srcLens[3]},
                    {line:6.5, pO: srcLens[3]},
                  ];
                  return lines.map((L,i)=>{
                    const over = L.pO;
                    const under = 1 - over;
                    // v86: settled detection corrected. Previously settledU/settledO were swapped:
                    // when over≥0.9999 (e.g. series tied 2-2 means OVER 4.5 will absolutely happen),
                    // we should set OVER=settled-yes (apO=1, apU=0), but old code set UNDER=settled.
                    const settledOverYes = over >= 0.9999 || (realized.gamesPlayed > L.line);
                    const settledOverNo  = over <= 0.0001;
                    let apO, apU;
                    if (settledOverYes) { apO = 1; apU = 0; }
                    else if (settledOverNo) { apO = 0; apU = 1; }
                    else { [apO,apU] = applyMargin([over,under], effMargins.length); }
                    const settled = settledOverYes || settledOverNo;
                    return (
                      <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:settled?0.55:1}}>
                        <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)"}}>{L.line}{settled&&<span style={{marginLeft:6,fontSize:9,color:settledOverYes?"#10b981":"#ef4444"}}>{settledOverYes?"OVER ✓":"UNDER ✓"}</span>}</td>
                        {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(over*100).toFixed(1)}%</td>}
                        {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(under*100).toFixed(1)}%</td>}
                        <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(apO*100).toFixed(1)}%</td>
                        <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(apU*100).toFixed(1)}%</td>
                        <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:apO>=0.5?"#4ade80":"var(--color-text-primary)"}}>{settled&&apO===0?"—":fmt(apO)}</td>
                        <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:apU>=0.5?"#4ade80":"var(--color-text-primary)"}}>{settled&&apU===0?"—":fmt(apU)}</td>
                      </tr>
                    );
                  });
                })()}</tbody>
              </table>
            </div>
          </Card>}

          {mkt==="winorder"&&(()=>{
            const hn=s.homeTeam||s.homeAbbr||"Home";
            const an=s.awayTeam||s.awayAbbr||"Away";
            // Convert H/A seq to team names
            const seqLabel=(seq)=>seq.split("").map(c=>c==="H"?hn:an).join(" / ");
            const copyAll=()=>{
              const txt=winOrders.map(o=>`${seqLabel(o.seq)}\t${o.ap>0?"+":""}${fmt(o.ap)}\t${toDec(o.ap).toFixed(2)}`).join("\n");
              navigator.clipboard?.writeText(txt);
            };
            return <Card>
              <div style={{display:"flex",alignItems:"center",marginBottom:10}}>
                <SH title="Win Order (70-Way)" sub={`OR: ${effMargins.winOrder}x — sequences show game-by-game winner`}/>
                <button onClick={copyAll} style={{marginLeft:"auto",padding:"3px 10px",fontSize:10,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>Copy All</button>
              </div>
              {realized.seriesOver && <div style={{padding:"6px 10px",marginBottom:8,background:"rgba(34,197,94,0.10)",border:"0.5px solid rgba(34,197,94,0.3)",borderRadius:"var(--border-radius-md)",fontSize:10,color:"#4ade80"}}>SERIES OVER — market settled</div>}
              <div style={{maxHeight:1000,overflowY:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <TH cols={["Sequence","Winner","Games",...(showTrue?["True%"]:[]),"Adj%","American","Dec"]}/>
                  <tbody>{winOrders.map((o,i)=>(
                    <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                      <td style={{padding:"3px 8px",fontSize:10}}>{seqLabel(o.seq)}{o._collapse&&<span style={{marginLeft:6,fontSize:8,padding:"1px 5px",borderRadius:2,background:"rgba(34,197,94,0.15)",color:"#4ade80",letterSpacing:0.3,fontWeight:500}} title="Priced at next-game moneyline (single-game collapse)">ML</span>}</td>
                      <td style={{padding:"3px 8px",fontSize:10,color:"var(--color-text-secondary)"}}>{o.hw===4?hn:an}</td>
                      <td style={{padding:"3px 8px",textAlign:"right",fontSize:10}}>{o.seq.length}</td>
                      {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(o.tp*100).toFixed(2)}%</td>}
                      <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(o.ap*100).toFixed(2)}%</td>
                      <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{fmt(o.ap)}</td>
                      <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{toDec(o.ap).toFixed(2)}</td>
                    </tr>
                  ))}</tbody>
                </table>
              </div>
            </Card>;
          })()}

          {mkt==="score3"&&<Card><SH title="Correct Score After 3 Games" sub={`OR: ${effMargins.correctScore}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Score",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody>{cs3.map((o,i)=>(
              <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:o._settled?0.4:1}}>
                <td style={{padding:"5px 8px"}}>{o.label}{o._settled&&<span style={{marginLeft:6,fontSize:9,color:o.tp>=0.5?"#10b981":"#ef4444"}}>{o.tp>=0.5?"✓":"✗"}</span>}</td>
                {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(o.tp*100).toFixed(1)}%</td>}
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.ap*100).toFixed(1)}%</td>
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:o.ap>=0.5?"#4ade80":"var(--color-text-primary)"}}>{o._settled?"—":fmt(o.ap)}</td>
                <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{o._settled?"—":toDec(o.ap).toFixed(2)}</td>
              </tr>
            ))}</tbody></table>
          </Card>}

          {mkt==="ot"&&<Card><SH title="OT Per Game" sub={`Per-game OT probability — OR: ${effMargins.otGames}x · Click "set" on played OT games to record scorer`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Game","Home","Win%","Total","pOT","OT Adj%","OT Odds","No OT Odds","OT Scorer"]}/>
              <tbody>{otPerGame.map((o,i)=>{
                const game = s.games?.[o.game-1];
                const otScorerName = game?.otScorer;
                const isOT = o._settled && o._wentOT;
                // Pool of players from the team that won this game (OT goal scored by winning team)
                const winningTeam = game?.result === "home" ? s.homeAbbr : game?.result === "away" ? s.awayAbbr : null;
                const winningPool = winningTeam ? (players||[]).filter(p => p.team === winningTeam && !isOutForSeries(p, s)) : [];
                return <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),opacity:o._settled?0.7:1}}>
                  <td style={{padding:"5px 8px",fontWeight:500}}>G{o.game}{o._settled&&<span style={{marginLeft:6,fontSize:9,color:o._wentOT?"#10b981":"#ef4444"}}>{o._wentOT?"OT ✓":"REG ✓"}</span>}</td>
                  <td style={{padding:"5px 8px",fontSize:10,color:"var(--color-text-secondary)"}}>{HOME_PATTERN[o.game]?(s.homeAbbr||"H"):(s.awayAbbr||"A")}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.winPct*100).toFixed(0)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{o.expTotal.toFixed(1)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(o.pOT*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.adjOT*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500}}>{o._settled?"—":fmt(o.adjOT)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{o._settled?"—":fmt(o.adjNo)}</td>
                  <td style={{padding:"5px 8px",fontSize:10}}>
                    {!isOT ? <span style={{color:"var(--color-text-tertiary)"}}>—</span> :
                     otScorerName ? <span style={{color:"#10b981"}}>{otScorerName} <button onClick={()=>{
                       setAllSeries(p=>{const u=[...p];const games=[...u[safeSi].games];games[o.game-1]={...games[o.game-1],otScorer:null};u[safeSi]={...u[safeSi],games};return u;});
                     }} style={{marginLeft:4,fontSize:9,padding:"0 4px",border:"none",background:"transparent",color:"var(--color-text-tertiary)",cursor:"pointer"}} title="Clear">✕</button></span> :
                     winningPool.length === 0 ? <span style={{color:"#f59e0b"}}>⚠ no roster</span> :
                     <select value="" onChange={e=>{
                       if (!e.target.value) return;
                       setAllSeries(p=>{const u=[...p];const games=[...u[safeSi].games];games[o.game-1]={...games[o.game-1],otScorer:e.target.value};u[safeSi]={...u[safeSi],games};return u;});
                     }} style={{padding:"2px 4px",fontSize:10,background:"var(--color-background-secondary)",border:"0.5px solid #f59e0b",borderRadius:3,color:"#f59e0b"}}>
                       <option value="">⚠ set scorer</option>
                       {winningPool.map(p => <option key={p.name} value={p.name}>{p.name}</option>)}
                     </select>}
                  </td>
                </tr>;
              })}</tbody>
            </table>
          </Card>}

          {mkt==="otseries"&&<Card>
            <SH title="OT Games in Series" sub={`λ=${otSeriesMkts.lambda.toFixed(2)} · Realized: ${realized.otGames}/${realized.gamesPlayed} games · Exact OR: ${effMargins.otExact}x · O/U OR: ${effMargins.otGames}x`}/>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
              <div>
                <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",marginBottom:6,textTransform:"uppercase"}}>Exact # OT Games</div>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <TH cols={["#OT",...(showTrue?["True%"]:[]),"Adj%","Odds"]}/>
                  <tbody>{(()=>{
                    const rows = otSeriesMkts.exactLines.map((k,i)=>({
                      k, tp: otSeriesMkts.exactProbs[i], ap: otSeriesMkts.exactAdj[i],
                      settled: otSeriesMkts.exactSettled[i],
                    }));
                    rows.sort((a,b)=>{
                      const aS = !!a.settled, bS = !!b.settled;
                      if (aS !== bS) return aS ? 1 : -1;
                      return a.k - b.k;
                    });
                    return rows.map((o,i)=>(
                      <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:o.settled?0.4:1}}>
                        <td style={{padding:"4px 8px"}}>Exactly {o.k}{o.settled&&<span style={{marginLeft:6,fontSize:9,color:o.settled==="yes"?"#10b981":"#ef4444"}}>{o.settled==="yes"?"✓":"✗"}</span>}</td>
                        {showTrue&&<td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(o.tp*100).toFixed(2)}%</td>}
                        <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(o.ap*100).toFixed(2)}%</td>
                        <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{o.settled?"—":fmt(o.ap)}</td>
                      </tr>
                    ));
                  })()}</tbody>
                </table>
              </div>
              <div>
                <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",marginBottom:6,textTransform:"uppercase"}}>O/U OT Games</div>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <TH cols={["Line",...(showTrue?["Over%"]:[]),"Ov Adj%","Over","Under"]}/>
                  <tbody>{otSeriesMkts.ouRows.map((r,i)=>(
                    <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:r._settled?0.4:1}}>
                      <td style={{padding:"4px 8px",fontFamily:"var(--font-mono)"}}>{r.line}{r._settled&&<span style={{marginLeft:6,fontSize:9,color:r._settledSide==="over"?"#10b981":"#ef4444"}}>{r._settledSide==="over"?"OVER ✓":"UNDER ✓"}</span>}</td>
                      {showTrue&&<td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                      <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                      <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{r._settled?"—":fmt(r.ao)}</td>
                      <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{r._settled?"—":fmt(r.au)}</td>
                    </tr>
                  ))}</tbody>
                </table>
              </div>
            </div>
          </Card>}

          {mkt==="spread"&&<Card><SH title="Series Spread" sub={`Wins differential — OR: ${effMargins.spread}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Home Line","Away Line",...(showTrue?["True%"]:[]),"H Adj%","H Odds","A Adj%","A Odds"]}/>
              <tbody>{spreadMkt.map((r,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),opacity:r._settled?0.4:1}}>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.homeLabel}{r._settled&&r._settledSide==="home"&&<span style={{marginLeft:6,fontSize:9,color:"#10b981"}}>✓</span>}{r._settled&&r._settledSide==="away"&&<span style={{marginLeft:6,fontSize:9,color:"#ef4444"}}>✗</span>}</td>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>{r.awayLabel}{r._settled&&r._settledSide==="away"&&<span style={{marginLeft:6,fontSize:9,color:"#10b981"}}>✓</span>}{r._settled&&r._settledSide==="home"&&<span style={{marginLeft:6,fontSize:9,color:"#ef4444"}}>✗</span>}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pHome*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ah*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.ah>=0.5?"#4ade80":"var(--color-text-primary)"}}>{r._settled?"—":fmt(r.ah)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.aa*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.aa>=0.5?"#4ade80":"var(--color-text-primary)"}}>{r._settled?"—":fmt(r.aa)}</td>
                </tr>
              ))}</tbody>
            </table>
          </Card>}

          {mkt==="totalgoals"&&<Card><SH title="Total Goals O/U" sub={`λ=${totalGoalsMkt.lambda.toFixed(2)} goals · OR: ${effMargins.totalGoals}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Line",...(showTrue?["P(Over)"]:[]),"Ov Adj%","Over","Under"]}/>
              <tbody>{totalGoalsMkt.lines.map((r,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),opacity:r._settled?0.4:1}}>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.line}{r._settled&&<span style={{marginLeft:6,fontSize:9,color:r._settledSide==="over"?"#10b981":"#ef4444"}}>{r._settledSide==="over"?"OVER ✓":"UNDER ✓"}</span>}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{r._settled?"—":fmt(r.ao)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{r._settled?"—":fmt(r.au)}</td>
                </tr>
              ))}</tbody>
            </table>
          </Card>}

          {mkt==="shutouts"&&<Card><SH title="Total Shutouts O/U" sub={`λ=${shutoutMkt.lambda.toFixed(3)} · Rate=${s.shutoutRate??0.08}/g · OR: ${effMargins.shutouts}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Line",...(showTrue?["P(Over)"]:[]),"Ov Adj%","Over","Under"]}/>
              <tbody>{shutoutMkt.lines.map((r,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:r._settled?0.4:1}}>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.line}{r._settled&&<span style={{marginLeft:6,fontSize:9,color:r._settledSide==="over"?"#10b981":"#ef4444"}}>{r._settledSide==="over"?"OVER ✓":"UNDER ✓"}</span>}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{r._settled?"—":fmt(r.ao)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{r._settled?"—":fmt(r.au)}</td>
                </tr>
              ))}</tbody>
            </table>
            {/* v80: exact shutout count market */}
            <div style={{marginTop:14,paddingTop:10,borderTop:"0.5px solid var(--color-border-tertiary)"}}>
              <div style={{fontSize:10,fontWeight:500,letterSpacing:"0.1em",textTransform:"uppercase",color:"var(--color-text-secondary)",marginBottom:6}}>Exact Shutouts</div>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <TH cols={["Count",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>{shutoutMkt.exactRows.map((r,i)=>(
                  <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:r._settled?0.4:1}}>
                    <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.label}</td>
                    {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.tp*100).toFixed(2)}%</td>}
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{r._settled?"—":(r.ap*100).toFixed(2)+"%"}</td>
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.ap>=0.5?"#4ade80":"var(--color-text-primary)"}}>{r._settled?"—":fmt(r.ap)}</td>
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{r._settled?"—":toDec(r.ap).toFixed(2)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </Card>}

          {mkt==="otscorer"&&(()=>{
            // v89: "OT Goal in Series" — interpretation (i): does player score AN OT goal in series.
            // Players who already scored an OT goal in this series are settled YES (sent to bottom, ✓).
            // Everyone else: priced as P(any OT goal among remaining games) × team OT-share × goal share.
            const eligibleRoles = new Set(["TOP6","MID6","BOT6","ACTIVE","ON_ROSTER","D1","D2","D3"]);
            const shrunkGoalRate = (p) => shrinkRate(p.g_pg || 0, p.gp || 0, "g");
            const teamGoalRate = (team) => {
              const pool = (players||[]).filter(p => p.team === team && eligibleRoles.has(p.lineRole));
              return pool.reduce((s,p) => s + shrunkGoalRate(p), 0) || 1;
            };
            const homeG = teamGoalRate(s.homeAbbr);
            const awayG = teamGoalRate(s.awayAbbr);
            // v89: realized OT scorers — count how many OT goals each player has in this series.
            const realizedOTByPlayer = {};
            for (const g of effG) {
              if (g.result && (g.wentOT || g.ot) && g.otScorer) {
                realizedOTByPlayer[g.otScorer] = (realizedOTByPlayer[g.otScorer] || 0) + 1;
              }
            }
            // FUTURE OT expectation per team — only over remaining (unplayed) games.
            // P(at least one OT in remaining games) and team-OT-win share for those.
            let pFutureOT = 0;
            const futureTeamOT = {home: 0, away: 0};
            for (let gi = 0; gi < effG.length; gi++) {
              const g = effG[gi];
              if (g.result) continue; // played games don't contribute to "future"
              const pPlayed = pGamePlayed[gi+1] ?? 0;
              const pOT = g.pOT ?? 0.22;
              pFutureOT += pPlayed * pOT;
              const wpOT = 0.5 + 0.6 * (g.winPct - 0.5);
              futureTeamOT.home += pPlayed * pOT * wpOT;
              futureTeamOT.away += pPlayed * pOT * (1 - wpOT);
            }
            pFutureOT = Math.min(1, pFutureOT);
            const pool = (players||[])
              .filter(p => (p.team === s.homeAbbr || p.team === s.awayAbbr) && eligibleRoles.has(p.lineRole));
            const rows = pool.map(p => {
              const teamRate = p.team === s.homeAbbr ? homeG : awayG;
              const share = teamRate > 0 ? shrunkGoalRate(p) / teamRate : 0;
              const roleMult =
                p.lineRole === "TOP6"   ? 1.20 :
                p.lineRole === "MID6"   ? 0.95 :
                p.lineRole === "BOT6"   ? 0.70 :
                p.lineRole === "ACTIVE" ? 0.85 :
                p.lineRole === "D1"     ? 1.00 :
                p.lineRole === "D2"     ? 0.75 :
                p.lineRole === "D3"     ? 0.55 : 0.60;
              const teamOT = p.team === s.homeAbbr ? futureTeamOT.home : futureTeamOT.away;
              // P(scores OT goal in REMAINING games)
              const rawP = Math.min(0.9999, share * roleMult * teamOT);
              const realizedOT = realizedOTByPlayer[p.name] || 0;
              return {...p, share, teamOT, rawP, realizedOT};
            });
            const or = effMargins.otScorer || 1.20;
            // Players with realized OT goals → settled YES (adjP = 1.0). Others → standard pricing.
            const totalRaw = rows.reduce((s,r)=>s+r.rawP, 0);
            const scale = totalRaw > 0 ? (pFutureOT / totalRaw) : 1;
            const adjusted = rows.map(r => {
              if (r.realizedOT > 0) return {...r, adjP: 1, _settled: true};
              return {...r, adjP: Math.min(0.9999, r.rawP * scale * or), _settled: false};
            }).filter(r => r._settled || r.rawP > 0.0005);
            // Sort: unsettled (active) first by adjP desc, settled (already-scored) at bottom.
            adjusted.sort((a,b) => {
              if (a._settled !== b._settled) return a._settled ? 1 : -1;
              if (a._settled) return b.realizedOT - a.realizedOT;
              return b.adjP - a.adjP;
            });
            return <Card>
              <SH title="Player to Score OT Goal in Series" sub={`Future OT remaining: ${(pFutureOT*100).toFixed(1)}% · OR: ${or}x · Top 40 active shown`}/>
              <div style={{marginBottom:8,fontSize:11,color:"var(--color-text-tertiary)"}}>
                Players who have already scored an OT goal in this series are <span style={{color:"#10b981"}}>locked YES (✓)</span> and sent to the bottom. Active players priced on remaining games only.
              </div>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <TH cols={["Player","Team","Role","Now",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>{adjusted.slice(0,60).map((r,i)=>(
                  <tr key={r.name+r.team} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:r._settled?"rgba(16,185,129,0.06)":(i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")),opacity:r._settled?0.85:1,textDecoration:r._settled?"line-through":"none"}}>
                    <td style={{padding:"3px 8px",textDecoration:"none"}}>
                      {r.name}
                      {r._settled&&<span style={{marginLeft:6,fontSize:10,color:"#10b981",textDecoration:"none"}}>✓</span>}
                    </td>
                    <td style={{padding:"3px 8px"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{r.team}</span></td>
                    <td style={{padding:"3px 8px"}}><RoleBadge role={r.lineRole}/></td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:r.realizedOT>0?"#10b981":"var(--color-text-tertiary)",fontWeight:r.realizedOT>0?500:400}}>{r.realizedOT||"—"}</td>
                    {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r._settled?"100.00":(r.rawP*100).toFixed(2)}%</td>}
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.adjP*100).toFixed(2)}%</td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.adjP>=0.05?"#4ade80":"var(--color-text-primary)",textDecoration:"none"}}>{r._settled?"—":fmt(r.adjP)}</td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)",textDecoration:"none"}}>{r._settled?"—":toDec(r.adjP).toFixed(2)}</td>
                  </tr>
                ))}</tbody>
              </table>
            </Card>;
          })()}

          {mkt==="firstotscorer"&&(()=>{
            // v89: First OT Goal Scorer — single 1-of-N market.
            // Once any OT happens in this series, the FIRST OT scorer is settled YES; everyone else
            // (including "No OT in series") is settled NO.
            // If no OT yet: standard market with "No OT scored" option = P(zero future OT).
            const eligibleRoles = new Set(["TOP6","MID6","BOT6","ACTIVE","ON_ROSTER","D1","D2","D3"]);
            const shrunkGoalRate = (p) => shrinkRate(p.g_pg || 0, p.gp || 0, "g");
            const teamGoalRate = (team) => {
              const pool = (players||[]).filter(p => p.team === team && eligibleRoles.has(p.lineRole));
              return pool.reduce((s,p) => s + shrunkGoalRate(p), 0) || 1;
            };
            const homeG = teamGoalRate(s.homeAbbr);
            const awayG = teamGoalRate(s.awayAbbr);
            // Find the FIRST realized OT scorer in this series (chronologically by gameNum).
            let firstOTScorer = null;
            for (const g of effG) {
              if (g.result && (g.wentOT || g.ot) && g.otScorer) { firstOTScorer = g.otScorer; break; }
            }
            // P(no future OT) — used for the "No OT scored" option when not settled.
            // computeOTSeriesPMF returns pmf where pmf[realizedOT] = P(no MORE OT going forward).
            const otPMFres = computeOTSeriesPMF(effG, 8);
            const pNoFutureOT = otPMFres.pmf[otPMFres.realizedOT] || 0;
            // pAnyFutureOT = 1 - pNoFutureOT — the probability mass to distribute among players
            // for "scores the first OT" if there's no realized OT yet.
            const pAnyFutureOT = Math.max(0, 1 - pNoFutureOT);
            // Per-team conditional share of WHICH team will get that first OT.
            // Approximation: weight by the team's share of expected future OT outcomes.
            let homeOTWeight = 0, awayOTWeight = 0;
            for (let gi = 0; gi < effG.length; gi++) {
              const g = effG[gi];
              if (g.result) continue;
              const pPlayed = pGamePlayed[gi+1] ?? 0;
              const pOT = g.pOT ?? 0.22;
              const wpOT = 0.5 + 0.6 * (g.winPct - 0.5);
              homeOTWeight += pPlayed * pOT * wpOT;
              awayOTWeight += pPlayed * pOT * (1 - wpOT);
            }
            const totalOTWeight = homeOTWeight + awayOTWeight || 1;
            const homeShareOfOT = homeOTWeight / totalOTWeight;
            const awayShareOfOT = awayOTWeight / totalOTWeight;
            const pool = (players||[])
              .filter(p => (p.team === s.homeAbbr || p.team === s.awayAbbr) && eligibleRoles.has(p.lineRole));
            const playerRows = pool.map(p => {
              const teamRate = p.team === s.homeAbbr ? homeG : awayG;
              const share = teamRate > 0 ? shrunkGoalRate(p) / teamRate : 0;
              const roleMult =
                p.lineRole === "TOP6"   ? 1.20 :
                p.lineRole === "MID6"   ? 0.95 :
                p.lineRole === "BOT6"   ? 0.70 :
                p.lineRole === "ACTIVE" ? 0.85 :
                p.lineRole === "D1"     ? 1.00 :
                p.lineRole === "D2"     ? 0.75 :
                p.lineRole === "D3"     ? 0.55 : 0.60;
              // P(this player scores the first OT goal) = P(any future OT) × P(this team gets it) × player goal share within team.
              const teamShareOfOT = p.team === s.homeAbbr ? homeShareOfOT : awayShareOfOT;
              const rawP = pAnyFutureOT * teamShareOfOT * share * roleMult;
              return {...p, share, rawP};
            });
            // Normalize so all-player tps sum exactly to pAnyFutureOT (since "No OT" = pNoFutureOT, total = 1).
            const sumPlayerRaw = playerRows.reduce((s,r)=>s+r.rawP, 0);
            const norm = sumPlayerRaw > 0 ? pAnyFutureOT / sumPlayerRaw : 0;
            const playerRowsNorm = playerRows.map(r => ({...r, tp: r.rawP * norm}));

            const or = effMargins.otScorer || 1.20;
            // Build all rows: [No OT Scored, ...players]
            const allRows = [
              {name: "No OT Scored", team: "—", lineRole: "—", tp: pNoFutureOT, _isNoOT: true},
              ...playerRowsNorm,
            ];
            // Settled handling
            const finalRows = allRows.map(r => {
              if (firstOTScorer != null) {
                // Already determined
                if (r._isNoOT) return {...r, adjP: 0, _settled: "no"};
                if (r.name === firstOTScorer) return {...r, adjP: 1, _settled: "yes"};
                return {...r, adjP: 0, _settled: "no"};
              }
              // Not yet determined — apply per-outcome juice
              return {...r, adjP: Math.min(0.9999, r.tp * or), _settled: null};
            }).filter(r => r._settled || r.tp > 0.0005 || r._isNoOT);
            finalRows.sort((a,b) => {
              if (a._settled === "yes" && b._settled !== "yes") return -1;
              if (b._settled === "yes" && a._settled !== "yes") return 1;
              if (a._settled === "no" && b._settled !== "no") return 1;
              if (b._settled === "no" && a._settled !== "no") return -1;
              if (a._isNoOT && !b._isNoOT) return -1;  // No OT first among unsettled
              if (b._isNoOT && !a._isNoOT) return 1;
              return b.adjP - a.adjP;
            });
            return <Card>
              <SH title="First OT Goal Scorer in Series" sub={firstOTScorer ? `LOCKED — ${firstOTScorer} scored the first OT goal` : `P(any OT in series): ${(pAnyFutureOT*100).toFixed(1)}% · OR: ${or}x · Includes "No OT" option`}/>
              <div style={{marginBottom:8,fontSize:11,color:"var(--color-text-tertiary)"}}>
                {firstOTScorer
                  ? <span>Market settled. <strong style={{color:"#10b981"}}>{firstOTScorer}</strong> scored the first OT goal — all other outcomes are dead.</span>
                  : <span>One winner — first player to score in OT. "No OT Scored" wins if the series ends without going to OT.</span>}
              </div>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <TH cols={["Outcome","Team","Role",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>{finalRows.slice(0,60).map((r,i)=>{
                  const settledYes = r._settled === "yes";
                  const settledNo = r._settled === "no";
                  return <tr key={r.name+r.team} style={{
                    borderBottom:"0.5px solid var(--color-border-tertiary)",
                    background:settledYes?"rgba(16,185,129,0.10)":(i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")),
                    opacity:settledNo?0.4:1,
                    textDecoration:settledNo?"line-through":"none",
                  }}>
                    <td style={{padding:"3px 8px",textDecoration:"none",fontWeight:r._isNoOT?500:400,color:r._isNoOT?"var(--color-text-secondary)":"var(--color-text-primary)"}}>
                      {r.name}
                      {settledYes&&<span style={{marginLeft:6,fontSize:10,color:"#10b981"}}>✓ WINNER</span>}
                    </td>
                    <td style={{padding:"3px 8px"}}>{r._isNoOT?<span style={{color:"var(--color-text-tertiary)",fontSize:10}}>—</span>:<span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{r.team}</span>}</td>
                    <td style={{padding:"3px 8px"}}>{r._isNoOT?<span style={{color:"var(--color-text-tertiary)",fontSize:10}}>—</span>:<RoleBadge role={r.lineRole}/>}</td>
                    {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.tp*100).toFixed(2)}%</td>}
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{settledYes?"100.00":settledNo?"0.00":(r.adjP*100).toFixed(2)}%</td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.adjP>=0.5?"#4ade80":"var(--color-text-primary)",textDecoration:"none"}}>{settledYes||settledNo?"—":fmt(r.adjP)}</td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)",textDecoration:"none"}}>{settledYes||settledNo?"—":toDec(r.adjP).toFixed(2)}</td>
                  </tr>;
                })}</tbody>
              </table>
            </Card>;
          })()}

          {mkt==="hattricks"&&(()=>{
            // v93: Hat Trick markets — two sub-markets in one tab.
            //   (1) Any Hat Trick in Series — Yes/No 2-way market
            //   (2) Player Hat Trick — per-player table, sortable
            // Both use realized credit: any player with a pGames entry where g >= 3 has
            // already hit a hat trick → settles YES for that player AND for the "any" market.
            //
            // Math:
            //   Per-game P(player scores >=3 goals) = 1 - poissonCDF(2, lambda_per_game)
            //   Across remaining games (assuming constant rate, independence):
            //     P(player hat trick in series) = 1 - (1 - pHatPerGame)^remainingGames
            //   "Any hat trick" = 1 - product over all players of (1 - pHatInSeries_player)
            const orHat = effMargins.hatTrick || 1.20;

            // Realized hat tricks (per player) — count games where this player scored 3+.
            // v102: e.round is a NUMBER (1/2/3/4), currentRound is a STRING ("r1"/"r2"/etc).
            //       Comparing them directly never matches → realized hat tricks always showed 0.
            //       Convert currentRound to round number for comparison.
            const _curRoundNum = currentRound === "r1" ? 1 : currentRound === "r2" ? 2 :
                                 currentRound === "conf" ? 3 : currentRound === "cup" ? 4 : null;
            const realizedHatByPlayer = {};
            const realizedAnyHat = (()=>{
              let any = false;
              for (const p of (players||[])) {
                if (!p.pGames) continue;
                if (p.team !== s.homeAbbr && p.team !== s.awayAbbr) continue;
                let count = 0;
                for (const e of p.pGames) {
                  // Filter to current round only (round numbers match)
                  if (_curRoundNum != null && (e.round||1) !== _curRoundNum) continue;
                  if ((e.g||0) >= 3) count++;
                }
                if (count > 0) {
                  realizedHatByPlayer[playerKey(p)] = count;
                  any = true;
                }
              }
              return any;
            })();

            // Per-player projection.
            const eligibleRoles = new Set(["TOP6","MID6","BOT6","ACTIVE","D2D","D1","D2","D3"]);
            const pool = (players||[])
              .filter(p => (p.team === s.homeAbbr || p.team === s.awayAbbr))
              .filter(p => !isOutForSeries(p, s))
              .filter(p => eligibleRoles.has(canonicalRole(effectiveRole(p, s))));

            const {rateDiscount} = globals;

            const rows = pool.map(p => {
              const role = effectiveRole(p, s);
              const rm = roleMultiplier(role, "g");
              const goalRateShrunk = shrinkRate(p.g_pg||0, p.gp||0, "g");
              const perGameLam = goalRateShrunk * rm * rateDiscount * statRateMultiplier("g");
              const roundGP = readActualGP(p, currentRound);
              const remainingGames = remainingGamesForPlayer(p, s, expG, roundGP);
              // Per-game P(score 3+) — Poisson tail
              const pHatPerGame = 1 - poissonPMF(0, perGameLam) - poissonPMF(1, perGameLam) - poissonPMF(2, perGameLam);
              // Across remaining games — independent across games
              const pHatInRemaining = remainingGames > 0
                ? 1 - Math.pow(1 - Math.max(0, pHatPerGame), remainingGames)
                : 0;
              const realized = realizedHatByPlayer[playerKey(p)] || 0;
              const settled = realized > 0;
              const tp = settled ? 1 : pHatInRemaining;
              return {
                p, role, perGameLam, pHatPerGame, pHatInRemaining,
                realized, _settled: settled, tp,
              };
            }).filter(r => r._settled || r.tp > 0.0005);

            // "Any hat trick" probability — product over all players of (1 - pHatInSeries)
            // Includes BOTH realized and projected players.
            // If any has been realized, P(any) = 1 already.
            let pAnyHat;
            if (realizedAnyHat) {
              pAnyHat = 1;
            } else {
              let pNoneHat = 1;
              for (const r of rows) pNoneHat *= (1 - r.tp);
              pAnyHat = 1 - pNoneHat;
            }
            const pNoHat = 1 - pAnyHat;
            // Apply margin: per-outcome juice for both sides
            const [adjAnyHat, adjNoHat] = realizedAnyHat
              ? [1, 0]
              : applyMargin([pAnyHat, pNoHat], orHat);

            // Sort: settled YES first, then by tp desc
            rows.sort((a,b) => {
              if (a._settled !== b._settled) return a._settled ? -1 : 1;
              return b.tp - a.tp;
            });

            return <Card>
              {/* Sub-market 1: Any Hat Trick */}
              <SH title="Any Hat Trick in Series" sub={realizedAnyHat ? "LOCKED YES — a hat trick has been scored" : `OR: ${orHat}x`}/>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12,marginBottom:14}}>
                <TH cols={["Outcome",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>
                  <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:realizedAnyHat?"rgba(16,185,129,0.08)":"transparent"}}>
                    <td style={{padding:"5px 8px"}}>
                      Yes (a hat trick will occur)
                      {realizedAnyHat && <span style={{marginLeft:6,fontSize:10,color:"#10b981"}}>✓ SETTLED</span>}
                    </td>
                    {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(pAnyHat*100).toFixed(1)}%</td>}
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(adjAnyHat*100).toFixed(1)}%</td>
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:adjAnyHat>=0.5?"#4ade80":"var(--color-text-primary)"}}>{realizedAnyHat?"—":fmt(adjAnyHat)}</td>
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{realizedAnyHat?"—":toDec(adjAnyHat).toFixed(2)}</td>
                  </tr>
                  <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:realizedAnyHat?0.4:1,textDecoration:realizedAnyHat?"line-through":"none"}}>
                    <td style={{padding:"5px 8px"}}>No (no hat trick)</td>
                    {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(pNoHat*100).toFixed(1)}%</td>}
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(adjNoHat*100).toFixed(1)}%</td>
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:adjNoHat>=0.5?"#4ade80":"var(--color-text-primary)",textDecoration:"none"}}>{realizedAnyHat?"—":fmt(adjNoHat)}</td>
                    <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)",textDecoration:"none"}}>{realizedAnyHat?"—":toDec(adjNoHat).toFixed(2)}</td>
                  </tr>
                </tbody>
              </table>

              {/* Sub-market 2: Player Hat Trick */}
              <div style={{paddingTop:6,borderTop:"0.5px solid var(--color-border-tertiary)"}}>
                <div style={{fontSize:10,fontWeight:500,letterSpacing:"0.1em",textTransform:"uppercase",color:"var(--color-text-secondary)",marginBottom:4}}>Player Hat Trick</div>
                <div style={{fontSize:11,color:"var(--color-text-tertiary)",marginBottom:8}}>
                  Player to score 3+ goals in any single game in this series. Players who already scored a hat trick are <span style={{color:"#10b981"}}>locked YES (✓)</span>.
                </div>
              </div>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <TH cols={["Player","Team","Role","Now",...(showTrue?["Per-G%","Series%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>{rows.slice(0,80).map((r,i)=>{
                  const adjP = r._settled ? 1 : Math.min(0.9999, r.tp * orHat);
                  return <tr key={r.p.name+r.p.team} style={{
                    borderBottom:"0.5px solid var(--color-border-tertiary)",
                    background:r._settled?"rgba(16,185,129,0.06)":(i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")),
                  }}>
                    <td style={{padding:"3px 8px"}}>
                      {r.p.name}
                      {r._settled && <span style={{marginLeft:6,fontSize:10,color:"#10b981"}}>✓</span>}
                    </td>
                    <td style={{padding:"3px 8px"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{r.p.team}</span></td>
                    <td style={{padding:"3px 8px"}}><RoleBadge role={r.role}/></td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:r.realized>0?"#10b981":"var(--color-text-tertiary)",fontWeight:r.realized>0?500:400}}>{r.realized||"—"}</td>
                    {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pHatPerGame*100).toFixed(2)}%</td>}
                    {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.tp*100).toFixed(2)}%</td>}
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{r._settled?"100.00":(adjP*100).toFixed(2)}%</td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:adjP>=0.05?"#4ade80":"var(--color-text-primary)"}}>{r._settled?"—":fmt(adjP)}</td>
                    <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r._settled?"—":toDec(adjP).toFixed(2)}</td>
                  </tr>;
                })}</tbody>
              </table>
            </Card>;
          })()}

          {mkt==="mostgoals"&&<Card><SH title="Team With Most Goals" sub={`Realized: ${s.homeAbbr||"H"} ${realized.goalsH}g · ${s.awayAbbr||"A"} ${realized.goalsA}g · OR: ${effMargins.teamMostGoals}x`}/>
            <div style={{marginBottom:10,fontSize:11,color:"var(--color-text-tertiary)"}}>Push (tie) pays full.</div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Outcome",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
              <tbody>
                <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:mostGoalsMkt._settled&&mostGoalsMkt._settledSide!=="home"?0.4:1}}>
                  <td style={{padding:"5px 8px"}}>{s.homeTeam||"Home"}{mostGoalsMkt._settled&&<span style={{marginLeft:6,fontSize:9,color:mostGoalsMkt._settledSide==="home"?"#10b981":"#ef4444"}}>{mostGoalsMkt._settledSide==="home"?"✓":"✗"}</span>}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(mostGoalsMkt.pHomeMost*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(mostGoalsMkt.ah*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:mostGoalsMkt.ah>=0.5?"#4ade80":"var(--color-text-primary)"}}>{mostGoalsMkt._settled?"—":fmt(mostGoalsMkt.ah)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{mostGoalsMkt._settled?"—":toDec(mostGoalsMkt.ah).toFixed(2)}</td>
                </tr>
                <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:mostGoalsMkt._settled&&mostGoalsMkt._settledSide!=="away"?0.4:1}}>
                  <td style={{padding:"5px 8px"}}>{s.awayTeam||"Away"}{mostGoalsMkt._settled&&<span style={{marginLeft:6,fontSize:9,color:mostGoalsMkt._settledSide==="away"?"#10b981":"#ef4444"}}>{mostGoalsMkt._settledSide==="away"?"✓":"✗"}</span>}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(mostGoalsMkt.pAwayMost*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(mostGoalsMkt.aa*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500,color:mostGoalsMkt.aa>=0.5?"#4ade80":"var(--color-text-primary)"}}>{mostGoalsMkt._settled?"—":fmt(mostGoalsMkt.aa)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{mostGoalsMkt._settled?"—":toDec(mostGoalsMkt.aa).toFixed(2)}</td>
                </tr>
                <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                  <td style={{padding:"5px 8px",color:"var(--color-text-secondary)"}}>Tied (Push)</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(mostGoalsMkt.pTied*100).toFixed(1)}%</td>}
                  <td colSpan={3} style={{padding:"5px 8px",textAlign:"right",fontSize:10,color:"var(--color-text-tertiary)"}}>void / push</td>
                </tr>
              </tbody>
            </table>
          </Card>}

          {mkt==="teamgoals"&&<Card><SH title="Per-Team Total Goals O/U" sub={`Realized: ${s.homeAbbr||"H"} ${realized.goalsH}g · ${s.awayAbbr||"A"} ${realized.goalsA}g · OR: ${effMargins.teamGoals}x`}/>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:0}}>
              {[["home",s.homeTeam||s.homeAbbr||"Home",teamGoalsMkt.home],["away",s.awayTeam||s.awayAbbr||"Away",teamGoalsMkt.away]].map(([side,name,data],si)=>(
                <div key={side} style={{
                  padding:"0 16px 0",
                  borderRight:si===0?`2px solid ${dark?"#2d3147":"#e2e8f0"}`:"none",
                  paddingLeft:si===1?16:0,
                }}>
                  <div style={{fontSize:11,fontWeight:500,color:side==="home"?"#60a5fa":"#a78bfa",marginBottom:8,paddingBottom:6,borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                    {name} <span style={{color:"var(--color-text-tertiary)",fontWeight:400,fontSize:10}}>λ={data.lambda.toFixed(2)}</span>
                  </div>
                  <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                    <TH cols={["Line",...(showTrue?["P(O)"]:[]),"Ov%","Over","Under"]}/>
                    <tbody>{data.rows.map((r,i)=>(
                      <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),opacity:r._settled?0.4:1}}>
                        <td style={{padding:"4px 6px",fontFamily:"var(--font-mono)"}}>{r.line}{r._settled&&<span style={{marginLeft:4,fontSize:9,color:r._settledSide==="over"?"#10b981":"#ef4444"}}>{r._settledSide==="over"?"O✓":"U✓"}</span>}</td>
                        {showTrue&&<td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                        <td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                        <td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{r._settled?"—":fmt(r.ao)}</td>
                        <td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{r._settled?"—":fmt(r.au)}</td>
                      </tr>
                    ))}</tbody>
                  </table>
                </div>
              ))}
            </div>
          </Card>}

          {mkt==="parlay"&&(()=>{
            const gNum = parlayMkt.gameNum;
            const gPlayed = s.games.filter(g=>g.result).length;
            const parlayTitle = gNum ? `Game ${gNum} × Series Winner Parlay` : "Parlay (series complete)";
            const parlayNote = gPlayed === 0
              ? "Next unplayed game result × series winner (4 combos)"
              : `Based on current series state (${s.games.filter(g=>g.result==="home").length}-${s.games.filter(g=>g.result==="away").length}). Next game × series winner.`;
            return <Card>
              <SH title={parlayTitle} sub={`OR: ${effMargins.parlay}x`}/>
              <div style={{marginBottom:8,fontSize:11,color:"var(--color-text-tertiary)"}}>{parlayNote}</div>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <TH cols={["Parlay",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>{parlayMkt.rows.map((r,i)=><OR key={i} label={r.label.replace("Home",s.homeTeam||"Home").replace("Away",s.awayTeam||"Away")} tp={r.tp} ap={r.ap} showTrue={showTrue}/>)}</tbody>
              </table>
            </Card>;
          })()}

          {mkt==="props"&&<PropsPanel s={s} expG={expG} gameGoalScale={gameGoalScale} gameEquivalents={gameEquivalents} gameEquivalentsFor={gameEquivalentsFor} players={players} globals={globals} margins={effMargins} showTrue={showTrue} dark={dark} mode="ou" simResult={simResult} currentRound={currentRound}/>}
          {mkt==="binary"&&<PropsPanel s={s} expG={expG} gameGoalScale={gameGoalScale} gameEquivalents={gameEquivalents} gameEquivalentsFor={gameEquivalentsFor} players={players} globals={globals} margins={effMargins} showTrue={showTrue} dark={dark} mode="binary" simResult={simResult} currentRound={currentRound}/>}
          {mkt==="propcombos"&&<PropCombosPanel s={s} expG={expG} gameGoalScale={gameGoalScale} gameEquivalents={gameEquivalents} gameEquivalentsFor={gameEquivalentsFor} players={players} globals={globals} margins={effMargins} showTrue={showTrue} dark={dark} currentRound={currentRound}/>}
          {mkt==="goaliesaves"&&<GoalieSavesPanel s={s} expG={expG} goalies={goalies} margins={effMargins} showTrue={showTrue} dark={dark} currentRound={currentRound}/>}
          {mkt==="playerdetail"&&<PlayerDetailPanel s={s} expG={expG} gameGoalScale={gameGoalScale} gameEquivalents={gameEquivalents} gameEquivalentsFor={gameEquivalentsFor} players={players} globals={globals} margins={effMargins} showTrue={showTrue} dark={dark} currentRound={currentRound}/>}
          {mkt==="seriesleader"&&<SeriesLeaderPanel s={s} expG={expG} gameGoalScale={gameGoalScale} gameEquivalents={gameEquivalents} gameEquivalentsFor={gameEquivalentsFor} players={players} globals={globals} margins={effMargins} showTrue={showTrue} dark={dark} simResult={simResult} simStale={simStale} currentRound={currentRound}/>}
        </div>
      </div>
    </div>
  );
}

// ─── PROPS PANEL (shared O/U + Binary) ───────────────────────────────────────
function PropsPanel({s,expG,gameGoalScale=1,gameEquivalents,gameEquivalentsFor,players,globals,margins,showTrue,dark,mode,simResult,currentRound}) {
  const [stat,setStat]=useState("g");
  const [line,setLine]=useState(mode==="binary"?1:0.5);
  const [expanded,setExpanded]=useState(()=>new Set()); // v24: expanded player keys for alt-line ladder
  const toggleExpand=(key)=>setExpanded(prev=>{const n=new Set(prev); n.has(key)?n.delete(key):n.add(key); return n;});
  const STATS=[
    {id:"g",label:"Goals",mk:"propsGoals"},{id:"a",label:"Assists",mk:"propsAssists"},
    {id:"pts",label:"Points",mk:"propsPoints"},{id:"sog",label:"SOG",mk:"propsSOG"},
    {id:"hit",label:"Hits",mk:"propsHits"},{id:"blk",label:"Blocks",mk:"propsBlocks"},
    {id:"tk",label:"Takeaways",mk:"propsTakeaways"},
    {id:"give",label:"Giveaways",mk:"propsGiveaways"},
  ];
  const statMeta=STATS.find(x=>x.id===stat)||STATS[0];
  const statMargin=margins[statMeta.mk]||1.05;

  const pool=useMemo(()=>{
    if(!players)return[];
    const teams=new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    return players.filter(p=>teams.has(p.team)&&!isOutForSeries(p, s));
  },[players,s.homeAbbr,s.awayAbbr]);

  const results=useMemo(()=>{
    const {rateDiscount,dispersion}=globals;
    // v46: stat-specific dispersion (scoring stats use ~Poisson, physical stats use overdispersed NB)
    const r = dispersionFor(stat, dispersion);
    // v24 Phase E: precompute sim index + points-PMF if sim is available and stat is simulated
    const simStats = new Set(["g","a","sog","hit","blk","tk","pim","give"]);
    const simIdx = simResult ? new Map(simResult.pool.map((sp,i)=>[sp.name+"|"+sp.team, i])) : null;
    return pool.map(p=>{
      const rm=roleMultiplier(p.lineRole, stat);
      // stat key mapping
      const pgKey=stat==="tk"?"take_pg":stat==="pim"?"pim_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
      // v13: shrink rate for <20 GP; scratched already filtered out above
      const shrunk=shrinkRate(p[pgKey],p.gp,stat);
      // v68: NO blend in Props. Goal is market-consensus matching; v67 blend caused
      // 200+ cent gaps on N+ markets (Kapanen 4+ -676 vs FD -115).
      // v21: stat-category rate adjustment (physical stats go up, scoring stays at baseline discount)
      const rm_rate_disc = shrunk*rm*rateDiscount*statRateMultiplier(stat);
      // v76: per-round GP and actual so R2 series doesn't carry over R1 stats
      const roundGP = readActualGP(p, currentRound);
      const remainingGames = remainingGamesForPlayer(p, s, expG, roundGP);
      const actual=readActual(p, stat, currentRound);
      // v16: for scoring stats, use per-game probability-weighted goal equivalents (respects per-game expTotal);
      // for defensive stats, fall back to flat expected-games-remaining.
      // v23: for scoring stats, gameEquivalentsFor(team, stat) additionally applies per-game goalie-faced multiplier.
      const gEq = SCORING_STATS.has(stat) && gameEquivalentsFor
        ? gameEquivalentsFor(p.team, stat)
        : (gameEquivalents!=null && SCORING_STATS.has(stat)) ? gameEquivalents : remainingGames;
      const futureLam = Math.max(0.0001, rm_rate_disc*gEq);
      const lam=Math.max(0.0001,actual+futureLam);
      // v22: settled logic.
      // Binary (X+): settled YES when actual >= line (irrevocable win).
      // O/U: settled OVER when actual > line (half-lines mean strict >).
      //       settled UNDER only if mathematically impossible to reach line — i.e., when no games remain (remainingGames===0) and actual < line.
      const settledYes = mode==="binary"
        ? actual >= line
        : actual > line;
      const settledNo = mode==="ou" && actual <= line && remainingGames <= 0 && gEq <= 0;
      const settled = settledYes || settledNo;
      // Only compute pOver for unsettled rows; settled rows get pOver=1 (if Yes) or 0 (if No)
      let pOver, effectiveLine, needMore;
      if (settledYes) { pOver = 1; effectiveLine = line; needMore = 0; }
      else if (settledNo) { pOver = 0; effectiveLine = line; needMore = Math.max(0, Math.ceil(line-0.001) - actual); }
      else {
        // v13 fix: For binary (1+), bump the line above what they already have.
        effectiveLine = mode==="binary" ? Math.max(line, actual+1) : line;
        const lineInt=Math.ceil(effectiveLine-0.001);
        needMore = Math.max(0, lineInt - actual);
        pOver = needMore===0 ? 1 : 1-nbCDF(needMore-1, futureLam, r);
      }
      // v24 Phase E: Unified-sim Over% for comparison. Uses the correlated sim's PMF.
      // For pts, sums G and A PMFs via convolution (since sim doesn't store pts PMF directly).
      let pSim = null;
      if (simIdx && (simStats.has(stat) || stat==="pts")) {
        const idx = simIdx.get(p.name+"|"+p.team);
        if (idx != null) {
          let pmf;
          if (stat === "pts") {
            // Convolve g and a PMFs to get pts PMF
            const gPMF = simResult.playerPMF[idx].g;
            const aPMF = simResult.playerPMF[idx].a;
            const maxK = gPMF.length + aPMF.length - 2;
            pmf = convolve(gPMF, aPMF, maxK);
          } else {
            pmf = simResult.playerPMF[idx][stat];
          }
          if (pmf) {
            if (settledYes) pSim = 1;
            else if (settledNo) pSim = 0;
            else {
              const lineInt = Math.ceil(effectiveLine - 0.001);
              const threshold = mode==="binary" ? Math.max(1, lineInt) : (Math.floor(effectiveLine)===effectiveLine ? lineInt+1 : lineInt);
              // pSim = P(total >= threshold); sim PMF is indexed by total, so sum tail
              let s = 0; for (let k = threshold; k < pmf.length; k++) s += pmf[k];
              pSim = s;
            }
          }
        }
      }
      // Apply margin only to uncertain events
      const [adjO,adjU]= settled || pOver>=0.9999 ? [pOver, 1-pOver] :
                         pOver<=0.0001 ? [0,1] : applyMargin([pOver,1-pOver],statMargin);
      return {...p,lam,futureLam,actual,effectiveLine,needMore,pYes:pOver,pSim,adjYes:adjO,adjNo:adjU,settled,settledYes,settledNo,remainingGames};
    }).sort((a,b)=>{
      // v49: settled rows drop to bottom; unsettled sort by adjYes desc
      if (!!a.settled !== !!b.settled) return a.settled ? 1 : -1;
      return b.adjYes - a.adjYes;
    });
  },[pool,stat,line,globals,expG,statMargin,gameEquivalents,mode,simResult,currentRound]);

  if(!s.homeAbbr||!s.awayAbbr) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Set team abbreviations to load props</div></Card>;

  const title=mode==="binary"
    ?`To Record ${line}+ ${statMeta.label}`
    :`O/U ${statMeta.label} — Line ${line}`;

  return <Card>
    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:10,flexWrap:"wrap"}}>
      <SH title={title} sub={`OR: ${statMargin}x · λ from reg season × ${globals.rateDiscount} discount`}/>
      <Seg options={STATS.map(s=>({id:s.id,label:s.label}))} value={stat} onChange={v=>{setStat(v);setLine(mode==="binary"?1:v==="sog"?2.5:v==="hit"?2.5:0.5);}} accent="#1d4ed8"/>
      <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center"}}>
        {mode==="binary"?"Min:":"Line:"} <LazyNI value={line} onCommit={setLine} min={mode==="binary"?1:0.5} max={50} step={mode==="binary"?1:0.5} style={{width:48}} showSpinner={false}/>
      </label>
      {mode==="ou" && <>
        <button onClick={()=>{
          if (expanded.size>0) setExpanded(new Set());
          else setExpanded(new Set(results.filter(r=>!r.settled).map(r=>r.name+"|"+r.team)));
        }} style={{fontSize:10,padding:"3px 8px",borderRadius:4,border:"0.5px solid var(--color-border-secondary)",background:"transparent",color:"var(--color-text-secondary)",cursor:"pointer"}}>{expanded.size>0?"Collapse all":"Expand all"}</button>
        <span style={{fontSize:10,color:"var(--color-text-tertiary)",fontStyle:"italic"}}>click a row to see alt lines</span>
      </>}
    </div>
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
        <TH cols={["#","Player","Team","Role","Now","Line","λ",...(showTrue?["True%"]:[]),...(simResult?["Sim%"]:[]),"Adj%","Yes Odds","No Odds"]}/>
        <tbody>{results.map((p,i)=>{
          // v22: use settled flag from result (definitive), distinguish settledYes vs settledNo
          const settled = p.settled;
          const settledYes = p.settledYes;
          const settledNo = p.settledNo;
          const rowOpacity = settled ? 0.45 : 1;
          const settleLabel = settledYes ? "✓ HIT" : settledNo ? "✗ MISS" : "";
          const settleColor = settledYes ? "#10b981" : settledNo ? "#ef4444" : "#64748b";
          // v24: alt-line ladder expansion (O/U only, not settled)
          const canExpand = mode==="ou" && !settled;
          const pKey = p.name+"|"+p.team;
          const isExp = expanded.has(pKey);
          const colCount = 10 + (showTrue?1:0) + (simResult?1:0);
          // Build ladder only when expanded (lazy)
          let ladder = null;
          if (canExpand && isExp) {
            const {dispersion} = globals;
            // v46: stat-specific dispersion
            const r = dispersionFor(stat, dispersion);
            const lam = p.lam;
            const futureLam = p.futureLam;
            const actual = p.actual;
            const remainingGames = p.remainingGames;
            const maxLine = Math.ceil(lam)+4;
            const rows = [];
            for (let l=0.5; l<=maxLine; l+=0.5) {
              const li = Math.ceil(l-0.001);
              const need = Math.max(0, li-actual);
              // CORRECT MATH: condition on actual (realized), model only the future.
              // line already cleared (actual > l) -> settled Over: pO=1
              // line unreachable (need > 0 AND no games/goals remain) -> settled Under: pO=0
              // otherwise: pOver = P(X_future >= need) = 1 - F(need-1; futureLam, r)
              const settledO = actual > l; // strict > for half-lines
              const settledU = need > 0 && remainingGames <= 0 && futureLam <= 0.0002;
              let pO;
              if (settledO) pO = 1;
              else if (settledU) pO = 0;
              else pO = need === 0 ? 1 : 1 - nbCDF(need-1, futureLam, r);
              const pU = 1 - pO;
              // No margin on settled lines
              const [ao,au] = (settledO || settledU || pO>=0.9999) ? [pO,pU]
                            : pO<=0.0001 ? [0,1]
                            : applyMargin([pO,pU],statMargin);
              rows.push({line:l,need,pO,pU,ao,au,settledO,settledU});
            }
            ladder = rows;
          }
          return (
          <Fragment key={i}>
          <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:settled?(dark?"rgba(100,116,139,0.08)":"rgba(100,116,139,0.05)"):i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),opacity:rowOpacity,cursor:canExpand?"pointer":"default"}} onClick={canExpand?()=>toggleExpand(pKey):undefined}>
            <td style={{padding:"3px 8px",color:"var(--color-text-tertiary)",fontSize:9}}>{canExpand?<span style={{display:"inline-block",width:10,color:"#60a5fa",marginRight:3,transform:isExp?"rotate(90deg)":"none",transition:"transform 120ms"}}>▸</span>:""}{i+1}</td>
            <td style={{padding:"3px 8px",fontWeight:settled?400:(i<3?500:400),textDecoration:settled?"line-through":"none"}}>{p.name}{settled?<span style={{marginLeft:6,fontSize:9,color:settleColor,fontWeight:500,textDecoration:"none"}}>{settleLabel}</span>:""}</td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><RoleBadge role={p.lineRole}/></td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,fontWeight:p.actual>0?500:400,color:p.actual>0?"#4ade80":"var(--color-text-tertiary)"}}>{p.actual||0}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:mode==="binary"&&p.effectiveLine>line?"#f59e0b":"var(--color-text-secondary)"}}>{mode==="binary"?`${p.effectiveLine}+`:p.line}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{p.lam.toFixed(2)}</td>
            {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{settled?"—":(p.pYes*100).toFixed(1)+"%"}</td>}
            {simResult&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:p.pSim!=null&&Math.abs(p.pSim-p.pYes)>0.02?"#f59e0b":"#a78bfa"}} title={p.pSim!=null?`Unified MC · Δ vs closed-form: ${((p.pSim-p.pYes)*100).toFixed(2)}pp`:"not simulated"}>{settled||p.pSim==null?"—":(p.pSim*100).toFixed(1)+"%"}</td>}
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{settled?"—":(p.adjYes*100).toFixed(1)+"%"}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:settled?"var(--color-text-tertiary)":(p.adjYes>=0.5?"#4ade80":"var(--color-text-primary)")}}>{settled?"—":fmt(p.adjYes)}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{settled?"—":fmt(p.adjNo)}</td>
          </tr>
          {ladder && <tr style={{background:dark?"rgba(59,130,246,0.04)":"rgba(59,130,246,0.03)"}}>
            <td colSpan={colCount} style={{padding:"6px 10px 10px 28px"}}>
              <div style={{fontSize:9,color:"var(--color-text-tertiary)",marginBottom:4,letterSpacing:0.4,textTransform:"uppercase"}}>Alt Lines · {p.name} · λ={p.lam.toFixed(3)} · OR={statMargin}x</div>
              <div style={{overflowX:"auto"}}>
                <table style={{borderCollapse:"collapse",fontSize:10,width:"auto"}}>
                  <thead><tr style={{color:"var(--color-text-tertiary)"}}>
                    <th style={{padding:"2px 8px",textAlign:"left",fontWeight:500}}>Line</th>
                    <th style={{padding:"2px 8px",textAlign:"right",fontWeight:500}}>Need</th>
                    {showTrue&&<th style={{padding:"2px 8px",textAlign:"right",fontWeight:500}}>True Ov</th>}
                    <th style={{padding:"2px 8px",textAlign:"right",fontWeight:500}}>Ov%</th>
                    <th style={{padding:"2px 8px",textAlign:"right",fontWeight:500}}>Un%</th>
                    <th style={{padding:"2px 8px",textAlign:"right",fontWeight:500}}>Over</th>
                    <th style={{padding:"2px 8px",textAlign:"right",fontWeight:500}}>Under</th>
                  </tr></thead>
                  <tbody>{ladder.map((r,j)=>{
                    const isKey = !r.settledO && !r.settledU && Math.abs(r.pO-0.5)<0.08;
                    const bg = r.settledO ? (dark?"rgba(16,185,129,0.08)":"rgba(16,185,129,0.06)")
                             : r.settledU ? (dark?"rgba(239,68,68,0.08)":"rgba(239,68,68,0.06)")
                             : isKey ? (dark?"rgba(59,130,246,0.1)":"rgba(59,130,246,0.06)")
                             : "transparent";
                    const lineColor = r.settledO ? "#10b981" : r.settledU ? "#ef4444" : isKey ? "#60a5fa" : "var(--color-text-primary)";
                    return <tr key={j} style={{background:bg}}>
                      <td style={{padding:"1px 8px",fontFamily:"var(--font-mono)",fontWeight:isKey||r.settledO||r.settledU?500:400,color:lineColor}}>{r.line.toFixed(1)}</td>
                      <td style={{padding:"1px 8px",textAlign:"right",fontFamily:"var(--font-mono)",color:r.need===0?"#4ade80":"var(--color-text-secondary)"}}>{r.need===0?"✓":r.need}</td>
                      {showTrue&&<td style={{padding:"1px 8px",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>{r.settledO?"100.0%":r.settledU?"0.0%":(r.pO*100).toFixed(1)+"%"}</td>}
                      <td style={{padding:"1px 8px",textAlign:"right",fontFamily:"var(--font-mono)"}}>{r.settledO?"100.0%":r.settledU?"0.0%":(r.ao*100).toFixed(1)+"%"}</td>
                      <td style={{padding:"1px 8px",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>{r.settledO?"0.0%":r.settledU?"100.0%":(r.au*100).toFixed(1)+"%"}</td>
                      <td style={{padding:"1px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500,color:r.settledO?"#10b981":r.ao>=0.5?"#4ade80":"var(--color-text-primary)"}}>{r.settledO?"✓ HIT":r.settledU?"—":fmt(r.ao)}</td>
                      <td style={{padding:"1px 8px",textAlign:"right",fontFamily:"var(--font-mono)",color:r.settledU?"#ef4444":"var(--color-text-secondary)"}}>{r.settledU?"✗ MISS":r.settledO?"—":fmt(r.au)}</td>
                    </tr>;
                  })}</tbody>
                </table>
              </div>
            </td>
          </tr>}
          </Fragment>
          );
        })}</tbody>
      </table>
    </div>
  </Card>;
}

// ─── PROP COMBOS PANEL (v73) ─────────────────────────────────────────────────
// 2-3 player combos with AND / OR / MORE operators across any stat with configurable thresholds.
// Independence assumption between players (simple, fine for first version per CC).
// MORE is 2-player only and supports 2-way (strict A>B) or 3-way with push (A>B / B>A / tie).
function PropCombosPanel({s,expG,gameGoalScale=1,gameEquivalents,gameEquivalentsFor,players,globals,margins,showTrue,dark,currentRound}) {
  const STATS=[
    {id:"g",label:"Goals",mk:"propsGoals"},{id:"a",label:"Assists",mk:"propsAssists"},
    {id:"pts",label:"Points",mk:"propsPoints"},{id:"sog",label:"SOG",mk:"propsSOG"},
    {id:"hit",label:"Hits",mk:"propsHits"},{id:"blk",label:"Blocks",mk:"propsBlocks"},
    {id:"tk",label:"Takeaways",mk:"propsTakeaways"},
    {id:"give",label:"Giveaways",mk:"propsGiveaways"},
  ];
  const pool = useMemo(()=>{
    if(!players)return[];
    const teams=new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    return players.filter(p=>teams.has(p.team)&&!isOutForSeries(p, s))
      .sort((a,b)=>(b.g_pg||0)-(a.g_pg||0));
  },[players,s.homeAbbr,s.awayAbbr]);

  // Slot state: each slot has {playerKey, stat, line}
  const [slots, setSlots] = useState([
    {playerKey:"", stat:"g", line:1},
    {playerKey:"", stat:"g", line:1},
  ]);
  const [op, setOp] = useState("OR"); // "OR" | "AND" | "H2H"
  const [moreMode, setMoreMode] = useState("2way"); // "2way" | "3way"

  const addSlot = () => {
    if (slots.length >= 5) return;
    setSlots(prev => [...prev, {playerKey:"", stat:"g", line:1}]);
    if (op === "H2H") setOp("OR"); // MORE not allowed with 3+
  };
  const removeSlot = (i) => {
    if (slots.length <= 2) return;
    setSlots(prev => prev.filter((_,j)=>j!==i));
  };

  // Per-slot lambda + PMF computation. Mirrors PropsPanel logic.
  const slotData = useMemo(()=>{
    const {rateDiscount,dispersion}=globals;
    return slots.map(slot => {
      if (!slot.playerKey) return null;
      const p = pool.find(x => `${x.name}|${x.team}` === slot.playerKey);
      if (!p) return null;
      const stat = slot.stat;
      const r = dispersionFor(stat, dispersion);
      const rm = roleMultiplier(p.lineRole, stat);
      if (rm === 0) return {p, stat, futureLam:0.0001, actual:0, r, lam:0.0001};
      const pgKey = stat==="tk"?"take_pg":stat==="pim"?"pim_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
      const shrunk = shrinkRate(p[pgKey], p.gp, stat);
      const rm_rate_disc = shrunk*rm*rateDiscount*statRateMultiplier(stat);
      const roundGP = readActualGP(p, currentRound);
      const remainingGames = remainingGamesForPlayer(p, s, expG, roundGP);
      const actual = readActual(p, stat, currentRound);
      const gEq = SCORING_STATS.has(stat) && gameEquivalentsFor
        ? gameEquivalentsFor(p.team, stat)
        : (gameEquivalents!=null && SCORING_STATS.has(stat)) ? gameEquivalents : remainingGames;
      const futureLam = Math.max(0.0001, rm_rate_disc*gEq);
      const lam = Math.max(0.0001, actual + futureLam);
      return {p, stat, futureLam, actual, r, lam};
    });
  }, [slots, pool, globals, expG, gameEquivalents, gameEquivalentsFor, currentRound]);

  // Per-slot P(X >= line) using NB CDF on FUTURE lambda
  const slotProbs = slotData.map((d, i) => {
    if (!d) return null;
    const line = slots[i].line;
    const lineInt = Math.ceil(line - 0.001);
    const needMore = Math.max(0, lineInt - d.actual);
    if (needMore === 0) return 1; // already has it
    return 1 - nbCDF(needMore - 1, d.futureLam, d.r);
  });

  // Build PMF for a slot's TOTAL (actual + future) — used for MORE comparisons.
  // Future is NB(futureLam, r); total = actual + future. Truncate at 30 for compute speed.
  const slotPMFs = slotData.map(d => {
    if (!d) return null;
    const KMAX = 30;
    const pmf = new Array(KMAX+1).fill(0);
    for (let k = 0; k <= KMAX; k++) {
      // future ≥ 0; total = actual + future. So future = k - actual.
      const fk = k - d.actual;
      if (fk < 0) continue;
      pmf[k] = nbPMF(fk, d.futureLam, d.r);
    }
    return pmf;
  });

  // Compute combo probability
  let trueProb = null;
  let pushProb = 0;
  let twoWayB = null; // for MORE, the "B more" prob

  const allSet = slotData.every(d => d != null);
  if (allSet) {
    if (op === "OR") {
      // 1 - Π(1 - p_i)
      let q = 1;
      for (const p of slotProbs) q *= (1 - p);
      trueProb = 1 - q;
    } else if (op === "AND") {
      let q = 1;
      for (const p of slotProbs) q *= p;
      trueProb = q;
    } else if (op === "H2H" && slotData.length === 2) {
      // P(A > B), P(A < B), P(A = B) by convolving over PMFs
      const [pmfA, pmfB] = slotPMFs;
      let pAGreater = 0, pBGreater = 0, pTie = 0;
      for (let a = 0; a < pmfA.length; a++) {
        for (let b = 0; b < pmfB.length; b++) {
          const j = pmfA[a] * pmfB[b];
          if (a > b) pAGreater += j;
          else if (b > a) pBGreater += j;
          else pTie += j;
        }
      }
      if (moreMode === "2way") {
        // Ties go to neither — typically book renormalizes. Assume "no push": ties reduce to nothing,
        // so renormalize over no-tie outcomes.
        const sum = pAGreater + pBGreater;
        trueProb = sum > 0 ? pAGreater / sum : 0.5;
        twoWayB = sum > 0 ? pBGreater / sum : 0.5;
      } else {
        trueProb = pAGreater;
        twoWayB = pBGreater;
        pushProb = pTie;
      }
    }
  }

  // Margin: use the strictest of involved stats' margins (or simple average). Default 1.05.
  const orMargin = useMemo(()=>{
    if (!allSet) return 1.05;
    const ms = slotData.map(d => margins[(STATS.find(x=>x.id===d.stat)||{}).mk] || 1.05);
    return Math.max(...ms);
  }, [slotData, allSet, margins]);

  let priceA = null, priceB = null, priceTie = null;
  if (trueProb != null) {
    if (op === "H2H" && moreMode === "3way") {
      // Apply margin proportionally across 3 outcomes
      const [adjA, adjB, adjT] = applyMargin([trueProb, twoWayB, pushProb], orMargin);
      priceA = adjA; priceB = adjB; priceTie = adjT;
    } else if (op === "H2H" && moreMode === "2way") {
      const [adjA, adjB] = applyMargin([trueProb, twoWayB], orMargin);
      priceA = adjA; priceB = adjB;
    } else {
      // 2-outcome (Yes / No)
      const [adjY, adjN] = applyMargin([trueProb, 1 - trueProb], orMargin);
      priceA = adjY; priceB = adjN;
    }
  }

  // v83: natural-language label for the combo. Also handles 3+ players with comma+ "&" formatting.
  const labelText = (() => {
    // Detect uniform stat + line across all slots (most common case → cleaner text).
    const validSlots = slotData.filter(d => d != null);
    if (validSlots.length === 0) return slots.map((_,i)=>`[Slot ${i+1}]`).join(" / ");

    const allSameStat = validSlots.every(d => d.stat === validSlots[0].stat);
    const lines = slotData.map((d,i) => slots[i] ? Math.ceil(slots[i].line - 0.001) : 1);
    const allSameLine = lines.filter((_,i)=>slotData[i]!=null).every(l => l === lines[slotData.findIndex(d=>d!=null)]);

    if ((op === "OR" || op === "AND") && allSameStat && allSameLine) {
      const statLabel = (STATS.find(x=>x.id===validSlots[0].stat)||{}).label || validSlots[0].stat;
      const lineInt = lines[slotData.findIndex(d=>d!=null)];
      const names = validSlots.map(d => d.p.name);
      // "A & B" for 2; "A, B & C" for 3+; "A, B, C & D" for 4+
      const namesText = names.length === 1 ? names[0]
        : names.length === 2 ? `${names[0]} ${op === "OR" ? "OR" : "AND"} ${names[1]}`
        : names.slice(0,-1).join(", ") + ` ${op === "OR" ? "OR" : "AND"} ${names[names.length-1]}`;
      // Lowercased plural form for stat — Goals → goals, Hits → hits
      const stLower = statLabel.toLowerCase();
      if (op === "OR") return `${namesText} to score ${lineInt}+ ${stLower} in series`;
      // AND with 2 players reads more naturally with "both"; 3+ with "each"
      const verb = names.length === 2 ? "both to score" : "each to score";
      return `${namesText} ${verb} ${lineInt}+ ${stLower} in series`;
    }
    if (op === "H2H" && validSlots.length === 2 && slotData[0] && slotData[1]) {
      const stat0 = (STATS.find(x=>x.id===slotData[0].stat)||{}).label || slotData[0].stat;
      return `${slotData[0].p.name} more ${stat0.toLowerCase()} than ${slotData[1].p.name}`;
    }
    // Heterogeneous fallback — list per-slot
    const parts = slotData.map((d,i) => {
      if (!d) return `[Slot ${i+1}]`;
      const statLabel = (STATS.find(x=>x.id===d.stat)||{}).label || d.stat;
      const lineInt = Math.ceil(slots[i].line - 0.001);
      return `${d.p.name} ${lineInt}+ ${statLabel.toLowerCase()}`;
    });
    return parts.join(op === "OR" ? " OR " : op === "AND" ? " AND " : " ?? ");
  })();

  return <Card>
    <SH title="Prop Combos" sub={`${s.homeAbbr || s.homeTeam || "Home"} vs ${s.awayAbbr || s.awayTeam || "Away"} · independence assumption`}/>

    {/* Operator selector */}
    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:12,flexWrap:"wrap"}}>
      <span style={{fontSize:11,color:"var(--color-text-tertiary)"}}>Operator:</span>
      {["OR","AND",...(slots.length===2?["H2H"]:[])].map(o => (
        <button key={o} onClick={()=>setOp(o)} style={{
          padding:"4px 12px",fontSize:11,fontWeight:500,
          background:op===o?"#1d4ed8":"var(--color-background-secondary)",
          color:op===o?"white":"var(--color-text-secondary)",
          border:"0.5px solid var(--color-border-secondary)",
          borderRadius:4,cursor:"pointer"}}>{o}</button>
      ))}
      {op === "H2H" && <>
        <span style={{fontSize:11,color:"var(--color-text-tertiary)",marginLeft:12}}>Mode:</span>
        {["2way","3way"].map(m => (
          <button key={m} onClick={()=>setMoreMode(m)} style={{
            padding:"4px 10px",fontSize:11,
            background:moreMode===m?"#7c3aed":"var(--color-background-secondary)",
            color:moreMode===m?"white":"var(--color-text-secondary)",
            border:"0.5px solid var(--color-border-secondary)",
            borderRadius:4,cursor:"pointer"}}>{m==="2way"?"2-way (no push)":"3-way (push)"}</button>
        ))}
      </>}
    </div>

    {/* Slot configurators */}
    <div style={{display:"grid",gap:8,marginBottom:14}}>
      {slots.map((slot, i) => (
        <div key={i} style={{display:"flex",gap:8,alignItems:"center",padding:8,
          background:"var(--color-background-secondary)",borderRadius:4,border:"0.5px solid var(--color-border-secondary)"}}>
          <span style={{fontSize:10,color:"var(--color-text-tertiary)",fontWeight:500,width:50}}>Player {i+1}</span>
          <select value={slot.playerKey} onChange={e=>{
            const v = e.target.value;
            setSlots(prev => prev.map((sl,j)=>j===i?{...sl, playerKey:v}:sl));
          }} style={{flex:1,padding:"4px 6px",fontSize:11,background:"var(--color-background-primary)",
            color:"var(--color-text-primary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,minWidth:160}}>
            <option value="">— select player —</option>
            {pool.map(p => <option key={`${p.name}|${p.team}`} value={`${p.name}|${p.team}`}>
              {p.name} ({p.team})
            </option>)}
          </select>
          <select value={slot.stat} onChange={e=>{
            const v = e.target.value;
            setSlots(prev => prev.map((sl,j)=>j===i?{...sl, stat:v}:sl));
          }} style={{padding:"4px 6px",fontSize:11,background:"var(--color-background-primary)",
            color:"var(--color-text-primary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3}}>
            {STATS.map(st => <option key={st.id} value={st.id}>{st.label}</option>)}
          </select>
          {op !== "H2H" && <>
            <span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>Line:</span>
            <input type="number" min="0.5" step="0.5" value={slot.line}
              onChange={e=>{
                const v = parseFloat(e.target.value)||0.5;
                setSlots(prev => prev.map((sl,j)=>j===i?{...sl, line:v}:sl));
              }}
              style={{width:50,padding:"4px 6px",fontSize:11,background:"var(--color-background-primary)",
                color:"var(--color-text-primary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,textAlign:"right"}}/>
            <span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>+</span>
          </>}
          {/* Per-slot diagnostic */}
          {slotData[i] && <span style={{fontSize:10,color:"var(--color-text-tertiary)",marginLeft:6,fontFamily:"var(--font-mono)"}}>
            λ {slotData[i].lam.toFixed(2)} (now {slotData[i].actual})
            {op !== "H2H" && slotProbs[i]!=null && ` · ${(slotProbs[i]*100).toFixed(1)}%`}
          </span>}
          {slots.length > 2 && <button onClick={()=>removeSlot(i)} style={{
            padding:"2px 8px",fontSize:11,background:"transparent",
            color:"#ef4444",border:"0.5px solid var(--color-border-secondary)",
            borderRadius:3,cursor:"pointer"}}>×</button>}
        </div>
      ))}
      {slots.length < 5 && <button onClick={addSlot} style={{
        padding:"6px 12px",fontSize:11,background:"transparent",
        color:"var(--color-text-secondary)",
        border:"1px dashed var(--color-border-secondary)",borderRadius:4,cursor:"pointer",width:"fit-content"}}>
        + Add player ({slots.length}/5)
      </button>}
    </div>

    {/* Result */}
    {trueProb != null ? <div style={{padding:12,background:"var(--color-background-secondary)",borderRadius:4,border:"0.5px solid var(--color-border-secondary)"}}>
      <div style={{fontSize:13,fontWeight:500,marginBottom:8}}>{labelText}</div>
      {op === "H2H" && moreMode === "3way" ? <table style={{width:"100%",fontSize:12}}>
        <thead><tr style={{color:"var(--color-text-tertiary)",fontSize:10,textAlign:"left"}}>
          <th style={{padding:"4px 0",fontWeight:400}}>Outcome</th>
          {showTrue && <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>True %</th>}
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>Adj %</th>
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>American</th>
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>Decimal</th>
        </tr></thead>
        <tbody>
          {[
            [`${slotData[0].p.name} more`, trueProb, priceA],
            [`${slotData[1].p.name} more`, twoWayB, priceB],
            [`Tie`, pushProb, priceTie],
          ].map(([n,tp,ap])=>(
            <tr key={n} style={{borderTop:"0.5px solid var(--color-border-tertiary)"}}>
              <td style={{padding:"6px 0"}}>{n}</td>
              {showTrue && <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-tertiary)"}}>{(tp*100).toFixed(1)}%</td>}
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)"}}>{(ap*100).toFixed(1)}%</td>
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500}}>{fmt(ap)}</td>
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)"}}>{toDec(ap).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table> : op === "H2H" ? <table style={{width:"100%",fontSize:12}}>
        <thead><tr style={{color:"var(--color-text-tertiary)",fontSize:10,textAlign:"left"}}>
          <th style={{padding:"4px 0",fontWeight:400}}>Outcome</th>
          {showTrue && <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>True %</th>}
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>Adj %</th>
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>American</th>
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>Decimal</th>
        </tr></thead>
        <tbody>
          {[
            [`${slotData[0].p.name} more`, trueProb, priceA],
            [`${slotData[1].p.name} more`, twoWayB, priceB],
          ].map(([n,tp,ap])=>(
            <tr key={n} style={{borderTop:"0.5px solid var(--color-border-tertiary)"}}>
              <td style={{padding:"6px 0"}}>{n}</td>
              {showTrue && <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-tertiary)"}}>{(tp*100).toFixed(1)}%</td>}
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)"}}>{(ap*100).toFixed(1)}%</td>
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500}}>{fmt(ap)}</td>
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)"}}>{toDec(ap).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table> : <table style={{width:"100%",fontSize:12}}>
        <thead><tr style={{color:"var(--color-text-tertiary)",fontSize:10,textAlign:"left"}}>
          <th style={{padding:"4px 0",fontWeight:400}}>Outcome</th>
          {showTrue && <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>True %</th>}
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>Adj %</th>
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>American</th>
          <th style={{padding:"4px 0",fontWeight:400,textAlign:"right"}}>Decimal</th>
        </tr></thead>
        <tbody>
          {[
            [`Yes`, trueProb, priceA],
            [`No`, 1-trueProb, priceB],
          ].map(([n,tp,ap])=>(
            <tr key={n} style={{borderTop:"0.5px solid var(--color-border-tertiary)"}}>
              <td style={{padding:"6px 0"}}>{n}</td>
              {showTrue && <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-tertiary)"}}>{(tp*100).toFixed(1)}%</td>}
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)"}}>{(ap*100).toFixed(1)}%</td>
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500}}>{fmt(ap)}</td>
              <td style={{padding:"6px 0",textAlign:"right",fontFamily:"var(--font-mono)"}}>{toDec(ap).toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>}
      <div style={{marginTop:8,fontSize:9,color:"var(--color-text-tertiary)"}}>
        Margin: {orMargin.toFixed(2)}x (max of involved stats)
      </div>
    </div> : <div style={{fontSize:12,color:"var(--color-text-tertiary)",fontStyle:"italic"}}>
      Select a player for each slot to see the combo price.
    </div>}
  </Card>;
}

// ─── GOALIE SAVES PANEL ──────────────────────────────────────────────────────
// Model: lambda = starter_share × saves_pg × expGames (Poisson O/U)
// Line auto-set to round(lambda) - 0.5 (nearest under). Matches Goalie Series Props sheet.
function GoalieSavesPanel({s,expG,goalies,margins,showTrue,dark,currentRound}) {
  const or = margins.propsGoals||1.05; // reuse saves margin setting
  // v102: round-aware goalie stats. Previously used cumulative g.pGP / g.pSaves,
  //       which bled R1 starts and saves into R2 series totals.
  const _curRoundNum = currentRound === "r1" ? 1 : currentRound === "r2" ? 2 :
                       currentRound === "conf" ? 3 : currentRound === "cup" ? 4 : null;
  const roundGoalieStats = (g) => {
    if (!Array.isArray(g.pGames) || _curRoundNum == null) {
      return { pGP: g.pGP || 0, pSaves: g.pSaves || 0 };
    }
    let pGP = 0, pSaves = 0;
    for (const e of g.pGames) {
      if (e.round !== _curRoundNum) continue;
      pGP++;
      pSaves += e.sv || 0;
    }
    return { pGP, pSaves };
  };

  const seriesGoalies = useMemo(()=>{
    if(!goalies) return [];
    const teams = new Set([s.homeAbbr, s.awayAbbr].filter(Boolean));
    if(!teams.size) return [];
    // v57: use user-assigned STARTER/BACKUP roles when available, else fall back to regular-season starter_share.
    //      Playoff start shares diverge sharply from regular season (backups sit), so role-first is more accurate.
    const teamStarterCount = {};
    const teamHasRoles = {};
    for (const g of goalies) {
      if (!teams.has(g.team)) continue;
      if (g.lineRole === "STARTER") {
        teamStarterCount[g.team] = (teamStarterCount[g.team] || 0) + 1;
        teamHasRoles[g.team] = true;
      } else if (g.lineRole === "BACKUP" || g.lineRole === "SCRATCHED" || g.lineRole === "INACTIVE" || g.lineRole === "IR") {
        teamHasRoles[g.team] = true;
      }
    }
    return goalies
      .filter(g => teams.has(g.team))
      .map(g => {
        let effectiveShare;
        if (teamHasRoles[g.team]) {
          // User set roles for this team — honor them strictly
          if (g.lineRole === "STARTER") {
            // Split equally if multiple STARTERs; else full 1.0
            effectiveShare = 1.0 / (teamStarterCount[g.team] || 1);
          } else {
            effectiveShare = 0;  // BACKUP/INACTIVE/IR — won't play unless starter injured
          }
        } else {
          // No roles set — use regular-season share as proxy
          effectiveShare = g.starter_share;
        }
        // v61: account for realized playoff saves + only project future saves from remaining games.
        //      Previously we always used `share * saves_pg * expG` which ignored saves already banked.
        //      Now: lam = realized_saves + (share * saves_pg * remainingGames_this_goalie)
        //      where remainingGames_this_goalie = expG - (g.pGP || 0)  [goalie has played pGP of the series already]
        // v102: pull round-filtered stats so R1 saves don't seed R2 series.
        const { pGP: roundGP, pSaves: roundSaves } = roundGoalieStats(g);
        const realizedSaves = roundSaves;
        const remainingGoalieGames = Math.max(0, expG - roundGP);
        const futureLam = Math.max(0, effectiveShare * (g.saves_pg || 0) * remainingGoalieGames);
        const lam = Math.max(0.0001, realizedSaves + futureLam);
        const autoLine = Math.max(0.5, Math.round(lam) - 0.5);
        return {...g, effectiveShare, lam, futureLam, realizedSaves, autoLine};
      })
      .filter(g => g.effectiveShare > 0 || g.realizedSaves > 0)  // v61: also show goalies with realized saves even if role=0 now
      .sort((a,b) => b.lam - a.lam);
  },[goalies, s.homeAbbr, s.awayAbbr, expG, currentRound]);

  if(!goalies) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Load goalies CSV in Upload tab to enable goalie saves props</div></Card>;
  if(!s.homeAbbr||!s.awayAbbr) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Set team abbreviations to load goalie props</div></Card>;
  if(!seriesGoalies.length) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>No goalies found for {s.homeAbbr} / {s.awayAbbr} — check team abbreviations match goalies CSV</div></Card>;

  return <Card>
    <SH title="Goalie Saves O/U" sub={`λ = starter_share × saves_pg × expGames · OR: ${or}x`}/>
    <div style={{fontSize:10,color:"var(--color-text-tertiary)",marginBottom:10}}>
      Exp series games: <strong style={{color:"var(--color-text-primary)"}}>{expG.toFixed(2)}</strong> · Line auto-set to round(λ)−0.5
    </div>
    {[s.homeAbbr, s.awayAbbr].map(abbr => {
      const teamGoalies = seriesGoalies.filter(g => g.team === abbr);
      if(!teamGoalies.length) return null;
      return (
        <div key={abbr} style={{marginBottom:20}}>
          <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",marginBottom:6,textTransform:"uppercase",letterSpacing:"0.08em"}}>{abbr} — {TEAM_NAMES[abbr]||abbr}</div>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <TH cols={["Goalie","Share","Sv/G","λ","Line",...(showTrue?["P(O)"]:[]),"Ov Adj%","Over","Under"]}/>
            <tbody>{teamGoalies.map((g,i)=>{
              const [ao,au] = applyMargin([1-poissonCDF(Math.ceil(g.autoLine-0.001)-1,g.lam), poissonCDF(Math.ceil(g.autoLine-0.001)-1,g.lam)], or);
              const pOver = 1-poissonCDF(Math.ceil(g.autoLine-0.001)-1, g.lam);
              return (
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),opacity:g.starter_share<0.05?0.45:1}}>
                  <td style={{padding:"4px 8px",fontWeight:g.starter_share>0.4?500:400}}>{g.name}</td>
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(g.starter_share*100).toFixed(1)}%</td>
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{g.saves_pg.toFixed(1)}</td>
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{g.lam.toFixed(1)}</td>
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500}}>{g.autoLine.toFixed(1)}</td>
                  {showTrue&&<td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(pOver*100).toFixed(1)}%</td>}
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(ao*100).toFixed(1)}%</td>
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:ao>=0.5?"#4ade80":"var(--color-text-primary)"}}>{fmt(ao)}</td>
                  <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(au)}</td>
                </tr>
              );
            })}</tbody>
          </table>
        </div>
      );
    })}
    <div style={{marginTop:8,padding:"6px 10px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",fontSize:10,color:"var(--color-text-tertiary)"}}>
      Goalies with &lt;5% starter share shown at reduced opacity — line not priced (insufficient starts).
      Playoff actual saves can be entered in Roles tab once playoffs begin.
    </div>
  </Card>;
}

// ─── PLAYER O/U DETAIL PANEL ─────────────────────────────────────────────────
// Full multi-line O/U table for a single selected player across all stats.
// Matches Player O-U Detail sheet: shows every half-integer line from 0.5 up.
function PlayerDetailPanel({s,expG,gameGoalScale=1,gameEquivalents,gameEquivalentsFor,players,globals,margins,showTrue,dark,currentRound}) {
  const [selectedPlayer,setSelectedPlayer] = useState("");
  const [stat,setStat] = useState("g");
  const STATS=[
    {id:"g",label:"Goals",mk:"propsGoals"},{id:"a",label:"Assists",mk:"propsAssists"},
    {id:"pts",label:"Points",mk:"propsPoints"},{id:"sog",label:"SOG",mk:"propsSOG"},
    {id:"hit",label:"Hits",mk:"propsHits"},{id:"blk",label:"Blocks",mk:"propsBlocks"},
    {id:"tk",label:"Takeaways",mk:"propsTakeaways"},
    {id:"give",label:"Giveaways",mk:"propsGiveaways"},
  ];
  const statMeta = STATS.find(x=>x.id===stat)||STATS[0];
  const or = margins[statMeta.mk]||1.05;

  const pool = useMemo(()=>{
    if(!players) return [];
    const teams = new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    return players.filter(p=>teams.has(p.team)&&!isOutForSeries(p, s)).sort((a,b)=>a.team.localeCompare(b.team)||b.pts-a.pts);
  },[players,s.homeAbbr,s.awayAbbr]);

  const player = pool.find(p=>p.name===selectedPlayer);

  const lines = useMemo(()=>{
    if(!player) return [];
    const {rateDiscount,dispersion} = globals;
    // v46: stat-specific dispersion
    const r = dispersionFor(stat, dispersion);
    const rm = roleMultiplier(player.lineRole, stat);
    const pgKey = stat==="tk"?"take_pg":stat==="pim"?"pim_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
    // v68: NO blend in Player Detail (alt-line table for N+ markets). Same reasoning as Props.
    const rr_base = shrinkRate(player[pgKey],player.gp,stat)*rm*rateDiscount*statRateMultiplier(stat);
    const roundGP = readActualGP(player, currentRound);
    const remainingGames = remainingGamesForPlayer(p, s, expG, roundGP);
    const gEq = SCORING_STATS.has(stat) && gameEquivalentsFor
      ? gameEquivalentsFor(player.team, stat)
      : (gameEquivalents!=null && SCORING_STATS.has(stat)) ? gameEquivalents : remainingGames;
    const actual = readActual(player, stat, currentRound);
    const futureLam = Math.max(0.0001, rr_base*gEq);
    const lam = Math.max(0.0001, actual + futureLam);
    // Build line table: 0.5 through ceil(lam)+4, step 0.5
    const maxLine = Math.ceil(lam)+4;
    const lineArr = [];
    for(let l=0.5; l<=maxLine; l+=0.5) {
      const li = Math.ceil(l-0.001);
      const need = Math.max(0, li-actual);
      // Conditional on actual: model only future, add realized
      const settledO = actual > l;
      const settledU = need > 0 && remainingGames <= 0 && futureLam <= 0.0002;
      let pOver;
      if (settledO) pOver = 1;
      else if (settledU) pOver = 0;
      else pOver = need===0 ? 1 : 1-nbCDF(need-1, futureLam, r);
      const pUnder = 1-pOver;
      const [ao,au] = (settledO||settledU||pOver>=0.9999) ? [pOver,pUnder]
                    : pOver<=0.0001 ? [0,1]
                    : applyMargin([pOver,pUnder],or);
      lineArr.push({line:l,pOver,pUnder,ao,au,lam,actual,need,settledO,settledU});
    }
    return {lam, rows: lineArr};
  },[player,stat,expG,globals,or,gameEquivalents,gameEquivalentsFor]);

  if(!s.homeAbbr||!s.awayAbbr) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Set team abbreviations first</div></Card>;

  return <Card>
    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:12,flexWrap:"wrap"}}>
      <SH title="Player O/U Detail" sub="Full line table for selected player"/>
      <select value={selectedPlayer} onChange={e=>setSelectedPlayer(e.target.value)}
        style={{...SEL, minWidth:180, fontSize:12}}>
        <option value="">— Select player —</option>
        {pool.map(p=><option key={p.name+p.team} value={p.name}>{p.name} ({p.team})</option>)}
      </select>
      <Seg options={STATS.map(x=>({id:x.id,label:x.label}))} value={stat} onChange={setStat} accent="#1d4ed8"/>
    </div>

    {!player && <div style={{color:"var(--color-text-secondary)",fontSize:12,padding:"20px 0",textAlign:"center"}}>Select a player above</div>}

    {player && lines.rows && <>
      <div style={{display:"flex",gap:16,marginBottom:12,padding:"8px 10px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",flexWrap:"wrap"}}>
        {[["Player",player.name],["Team",player.team],["Role",player.lineRole||"—"],["λ",lines.lam.toFixed(3)],["Actual",player[stat==="tk"?"pTK":stat==="pim"?"pPIM":"p"+stat.toUpperCase()]||0],["Exp Rem",expG.toFixed(2)+"g"],["OR",or+"x"]].map(([k,v])=>(
          <div key={k} style={{fontSize:10}}>
            <span style={{color:"var(--color-text-tertiary)"}}>{k}: </span>
            <span style={{fontFamily:"var(--font-mono)",fontWeight:500,color:"var(--color-text-primary)"}}>{v}</span>
          </div>
        ))}
      </div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <TH cols={["Line","Actual","Need",...(showTrue?["P(Over)","P(Under)"]:[]),"Ov Adj%","Un Adj%","Over","Under"]}/>
          <tbody>{lines.rows.map((r,i)=>{
            // Highlight the auto-line row (closest to 50/50)
            const isKey = !r.settledO && !r.settledU && Math.abs(r.pOver-0.5)<0.08;
            const bg = r.settledO ? (dark?"rgba(16,185,129,0.08)":"rgba(16,185,129,0.06)")
                     : r.settledU ? (dark?"rgba(239,68,68,0.08)":"rgba(239,68,68,0.06)")
                     : isKey ? (dark?"rgba(59,130,246,0.08)":"rgba(59,130,246,0.05)")
                     : i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)");
            const lineColor = r.settledO?"#10b981":r.settledU?"#ef4444":isKey?"#60a5fa":"var(--color-text-primary)";
            return (
              <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:bg}}>
                <td style={{padding:"3px 8px",fontFamily:"var(--font-mono)",fontWeight:isKey||r.settledO||r.settledU?500:400,color:lineColor}}>{r.line.toFixed(1)}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r.actual}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:r.need===0?"#4ade80":"var(--color-text-secondary)"}}>{r.need===0?"✓":r.need}</td>
                {showTrue&&<>
                  <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r.settledO?"100.0%":r.settledU?"0.0%":(r.pOver*100).toFixed(1)+"%"}</td>
                  <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r.settledO?"0.0%":r.settledU?"100.0%":(r.pUnder*100).toFixed(1)+"%"}</td>
                </>}
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{r.settledO?"100.0%":r.settledU?"0.0%":(r.ao*100).toFixed(1)+"%"}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r.settledO?"0.0%":r.settledU?"100.0%":(r.au*100).toFixed(1)+"%"}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.settledO?"#10b981":r.ao>=0.5?"#4ade80":"var(--color-text-primary)"}}>{r.settledO?"✓ HIT":r.settledU?"—":fmt(r.ao)}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:r.settledU?"#ef4444":"var(--color-text-secondary)"}}>{r.settledU?"✗ MISS":r.settledO?"—":fmt(r.au)}</td>
              </tr>
            );
          })}</tbody>
        </table>
      </div>
    </>}
  </Card>;
}


// Dead-heat leader market for both series teams. Per-stat temperature (power factor).
// Matches Leader View sheet: separate overround + per-stat temperature inputs.
const SERIES_LEADER_STATS = [
  {id:"g",    label:"Goals",     temp:1.1, kMax:12},
  {id:"a",    label:"Assists",   temp:1.3, kMax:15},
  {id:"pts",  label:"Points",    temp:1.3, kMax:20},
  {id:"sog",  label:"SOG",       temp:1.3, kMax:40},  // v65: lowered from 2.0 — was over-concentrating into 90% cap, defeating the temp control
  {id:"hit",  label:"Hits",      temp:1.5, kMax:50},
  {id:"blk",  label:"Blocks",    temp:1.5, kMax:40},
  {id:"tk",   label:"TK",        temp:1.5, kMax:20},
  {id:"give", label:"GV",        temp:1.5, kMax:30},
  {id:"toi",  label:"TOI",       temp:3.0, kMax:300}, // v56: series TOI leader (in minutes)
  ];

function SeriesLeaderPanel({s,expG,gameGoalScale=1,gameEquivalents,gameEquivalentsFor,players,globals,margins,showTrue,dark,simResult,simStale,currentRound}) {
  const [stat,setStat]=useState("g");
  const [customTemps,setCustomTemps]=useState({});
  // v90: team filter — "all" (default), "home", or "away".
  // Filtering to a single team makes this a within-team leader market (e.g., "PIT goal leader").
  // When filtered, the leader-probability normalization is computed only over that team's players,
  // so probs sum to 1 within team, and apply the standard overround.
  const [teamFilter, setTeamFilter] = useState("all");
  const meta=SERIES_LEADER_STATS.find(x=>x.id===stat)||SERIES_LEADER_STATS[0];
  const temp=customTemps[stat]??meta.temp;
  const or=margins.seriesLeader||1.15;

  const pool=useMemo(()=>{
    if(!players)return[];
    const teams=new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    let p = players.filter(p=>teams.has(p.team)&&!isOutForSeries(p, s));
    // v90: apply team filter
    if (teamFilter === "home" && s.homeAbbr) p = p.filter(x => x.team === s.homeAbbr);
    else if (teamFilter === "away" && s.awayAbbr) p = p.filter(x => x.team === s.awayAbbr);
    return p;
  },[players,s.homeAbbr,s.awayAbbr,teamFilter]);

  const leaderRows=useMemo(()=>{
    if(!pool.length)return[];
    const {rateDiscount,dispersion}=globals;
    // v24 Phase E: if unified sim is available AND this stat is simulated, use its leader probs (correct teammate correlation).
    // Otherwise fall back to independent-player MC (simulateLeader).
    const simStats = new Set(["g","a","pts","sog","hit","blk","tk","pim","give"]);
    const useUnified = simResult && !simStale && simResult.leaderProb && simStats.has(stat);

    // Build per-player lambda (for display), actual (for Now column), futureLam (for fallback)
    const entries=pool.map(p=>{
      const rm=roleMultiplier(p.lineRole, stat);
      const pgKey=stat==="tk"?"take_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat==="pim"?"pim_pg":stat+"_pg";
      const rr_base=shrinkRate(p[pgKey],p.gp,stat)*rm*rateDiscount*statRateMultiplier(stat);
      const roundGP = readActualGP(p, currentRound);
      const remainingGames = remainingGamesForPlayer(p, s, expG, roundGP);
      const gEq = SCORING_STATS.has(stat) && gameEquivalentsFor
        ? gameEquivalentsFor(p.team, stat)
        : (gameEquivalents!=null && SCORING_STATS.has(stat)) ? gameEquivalents : remainingGames;
      const actual = readActual(p, stat, currentRound);
      const futureLam = Math.max(0.0001, rr_base*gEq);
      return {actual, futureLam, lam: actual+futureLam};
    });

    let raw;
    if (useUnified) {
      // Map pool -> simResult index by name+team
      const simIdx = new Map(simResult.pool.map((sp,i)=>[sp.name+"|"+sp.team, i]));
      const simProbs = simResult.leaderProb[stat] || [];
      raw = pool.map(p=>{
        const idx = simIdx.get(p.name+"|"+p.team);
        return idx==null ? 0 : (simProbs[idx] || 0);
      });
    } else {
      const seed = 54321 + stat.charCodeAt(0);
      // v46: stat-specific dispersion (scoring stats use ~Poisson)
      const r = dispersionFor(stat, dispersion);
      raw = simulateLeader(entries.map(e=>({futureLam:e.futureLam,actual:e.actual})), r, 10000, seed);
    }

    // v90: when team-filtered, the surviving probs sum to less than 1 (the rest of the mass
    // belongs to the other team). Renormalize to sum to 1 within the filtered subset so the
    // "leads this team" market makes sense. This is an approximation — the sim measures
    // "leads the whole series", but conditional on filtering, the dominant player in a team
    // is by far most likely to also be the team-leader, so renormalizing is a reasonable proxy.
    if (teamFilter !== "all") {
      const sumRaw = raw.reduce((a,b)=>a+b, 0);
      if (sumRaw > 0) raw = raw.map(p => p / sumRaw);
    }

    // v49: principled leader overround — cap favorites, redistribute overflow to longshots
    const adj = applyLeaderOverround(raw, temp, or);
    return pool.map((p,i)=>({...p,lambda:entries[i].lam,futureLam:entries[i].futureLam,actualStat:entries[i].actual,trueProb:raw[i],adjProb:adj[i]}))
      .sort((a,b)=>b.adjProb-a.adjProb);
  },[pool,stat,expG,globals,temp,or,gameEquivalents,gameEquivalentsFor,simResult,currentRound,teamFilter]);

  if(!s.homeAbbr||!s.awayAbbr) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Set team abbreviations to load series leaders</div></Card>;

  const engineLabel = simResult && simResult.leaderProb ? `Unified MC (${simResult.trials/1000}k, L1)` : "Independent MC (10k)";

  return <Card>
    {/* v66: SH on its own row, then controls row. Prevents subtitle from overlapping adjacent card. */}
    <div style={{marginBottom:10}}>
      <SH title="Series Stat Leader" sub={`${engineLabel} · OR: ${or}x${teamFilter!=="all"?` · ${teamFilter==="home"?(s.homeTeam||s.homeAbbr||"Home"):(s.awayTeam||s.awayAbbr||"Away")} only`:""}`}/>
      {/* v90: team filter — pricing within a single team turns this into a within-team leader market. */}
      <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:8,flexWrap:"wrap"}}>
        <span style={{fontSize:10,color:"var(--color-text-tertiary)",letterSpacing:0.3}}>TEAM</span>
        <Seg options={[
          {id:"all",label:"All"},
          ...(s.homeAbbr?[{id:"home",label:s.homeTeam||s.homeAbbr}]:[]),
          ...(s.awayAbbr?[{id:"away",label:s.awayTeam||s.awayAbbr}]:[]),
        ]} value={teamFilter} onChange={setTeamFilter}/>
      </div>
      <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
        <Seg options={SERIES_LEADER_STATS.map(s=>({id:s.id,label:s.label}))} value={stat} onChange={setStat} accent="#7c3aed"/>
        <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:4,alignItems:"center"}}>
          Temp: <LazyNI value={temp} onCommit={v=>setCustomTemps(t=>({...t,[stat]:v}))} min={0.5} max={5} step={0.1} style={{width:46}} showSpinner={false}/>
        </label>
        {simResult && <span style={{fontSize:9,padding:"2px 6px",borderRadius:3,background:"rgba(124,58,237,0.15)",color:"#a78bfa",letterSpacing:0.4,fontWeight:500}}>UNIFIED</span>}
      </div>
    </div>
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
        <TH cols={["#","Player","Team","Role","Now","λ",...(showTrue?["True%","DH%"]:[]),"Adj%","American","Dec"]}/>
        <tbody>{leaderRows.map((p,i)=>{
          const a=toAmer(p.adjProb);
          // current actual series stat (v76: per-round)
          const now=readActual(p, stat, currentRound);
          return <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
            <td style={{padding:"3px 8px",color:"var(--color-text-tertiary)",fontSize:9}}>{i+1}</td>
            <td style={{padding:"3px 8px",fontWeight:i<3?500:400}}>{p.name}</td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(124,58,237,0.15)",color:"#a78bfa"}}>{p.team}</span></td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><RoleBadge role={p.lineRole}/></td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:now>0?500:400,color:now>0?"#4ade80":"var(--color-text-tertiary)"}}>{now>0?now:"—"}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{p.lambda.toFixed(2)}</td>
            {showTrue&&<><td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(p.trueProb*100).toFixed(2)}%</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(Math.pow(p.trueProb,temp)*100).toFixed(2)}%</td></>}
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>
              {(p.adjProb*100).toFixed(2)}%
              {p.adjProb >= 0.949 && <span title="Hit MAX_PROB cap (0.95) — increasing temp won't move this row further. Lower temp instead." style={{marginLeft:4,fontSize:8,padding:"0px 3px",background:"rgba(245,158,11,0.18)",color:"#f59e0b",borderRadius:2}}>CAP</span>}
            </td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:a<0?"#4ade80":"var(--color-text-primary)"}}>{a>0?`+${a}`:a}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{toDec(p.adjProb).toFixed(2)}</td>
          </tr>;
        })}</tbody>
      </table>
    </div>
  </Card>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// LINE COMPARE TAB
// ═══════════════════════════════════════════════════════════════════════════════
// Parser for book odds paste. Format: "Player Name +ODDS" or "Player Name -ODDS"
// Handles: American odds (+390, -150), decimal (3.90), loose text/headers ignored.
function parseBookPaste(text, playerPool) {
  // Extract all "Name +/-NNNN" or "Name NNNN" patterns
  // Strategy: find tokens matching american odds (+100 to +99999, -9999 to -100)
  const lines = text.replace(/\s{2,}/g,' ').split(/[\n\r]+/);
  const results = [];
  const oddsRe = /([+-]\d{2,5})/g;
  const nameOddsRe = /^(.+?)\s+([+-]\d{2,5})\s*$/;

  // Build fuzzy match index from player pool
  function normalize(s){ return s.toLowerCase().replace(/[^a-z]/g,''); }
  const poolIndex = playerPool.map(p=>({...p, norm:normalize(p.name)}));

  function fuzzyMatch(name) {
    const n = normalize(name);
    // Exact normalized match first
    let m = poolIndex.find(p=>p.norm===n);
    if(m) return m;
    // Last name match (avoid false positives — require last name >= 4 chars)
    const parts = name.trim().split(/\s+/);
    const last = normalize(parts[parts.length-1]);
    if(last.length>=4){
      const lastMatches = poolIndex.filter(p=>p.norm.endsWith(last)||p.norm.includes(last));
      if(lastMatches.length===1) return lastMatches[0];
    }
    // Starts-with match on full normalized name
    if(n.length>=6){
      const sw = poolIndex.filter(p=>p.norm.startsWith(n.slice(0,6))||n.startsWith(p.norm.slice(0,6)));
      if(sw.length===1) return sw[0];
    }
    return null;
  }

  // Process line by line — each line may have "Name +ODDS" or just be header noise
  let buf = "";
  for(const raw of lines){
    const line = raw.trim();
    if(!line) continue;
    // Try direct "Name ODDS" pattern first
    const m = line.match(nameOddsRe);
    if(m){
      const candidate = m[1].trim();
      const odds = parseInt(m[2]);
      if(Math.abs(odds)>=100 && Math.abs(odds)<=99999){
        const player = fuzzyMatch(candidate);
        if(player) results.push({player, odds, raw:line, matched:true});
        else results.push({player:null, odds, raw:line, matched:false, candidateName:candidate});
      }
      continue;
    }
    // No direct match — try to find odds token anywhere in line
    const oddsTokens = [...line.matchAll(/([+-]\d{2,5})/g)];
    for(const tok of oddsTokens){
      const odds = parseInt(tok[1]);
      if(Math.abs(odds)<100||Math.abs(odds)>99999) continue;
      // Name is everything before the odds token
      const namePart = line.slice(0, tok.index).trim().replace(/[^a-zA-Z\s'.]/g,'').trim();
      if(namePart.split(/\s+/).length<2) continue; // need at least first + last
      const player = fuzzyMatch(namePart);
      if(player) results.push({player, odds, raw:line, matched:true});
      else results.push({player:null, odds, raw:line, matched:false, candidateName:namePart});
    }
  }

  // Deduplicate — if same player matched multiple times, keep first
  const seen = new Set();
  const deduped = results.filter(r=>{
    if(!r.matched||!r.player) return true;
    const key=r.player.name;
    if(seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  return deduped;
}

function amerToDecimal(amer){
  if(amer>0) return +(1+100/amer).toFixed(3);
  if(amer<0) return +(1+100/Math.abs(amer)).toFixed(3);
  return null;
}

function CompareTab({leaderMarket,STATS,lStat,setLStat,lScope,setLScope,dark}) {
  const BOOKS=["FanDuel","BetMGM","DraftKings","Pinnacle","Bet365"];
  // bookOdds[bookName][playerName] = { amer, dec } (american int + decimal float)
  const [bookOdds,setBookOdds]=useState({});
  const [activeBook,setActiveBook]=useState("FanDuel");
  const [pasteText,setPasteText]=useState("");
  const [parseResult,setParseResult]=useState(null); // {matched,unmatched,book}
  const [showPastePanel,setShowPastePanel]=useState(true);
  const [showAll,setShowAll]=useState(true);
  const [filterTeam,setFilterTeam]=useState("ALL");
  const [search,setSearch]=useState("");

  const teams=[...new Set(leaderMarket.map(p=>p.team))].sort();

  function getOdds(book,name){ return bookOdds[book]?.[name]||null; }
  function setPlayerOdds(book,name,amer,dec){
    setBookOdds(prev=>({...prev,[book]:{...(prev[book]||{}),[name]:{amer,dec}}}));
  }
  function clearBook(book){ setBookOdds(prev=>({...prev,[book]:{}})); }

  function handleParse(){
    if(!pasteText.trim()) return;
    const pool = leaderMarket; // already computed players with name/team
    const parsed = parseBookPaste(pasteText, pool);
    let matched=0, unmatched=[];
    for(const r of parsed){
      if(r.matched&&r.player){
        const dec = amerToDecimal(r.odds);
        setPlayerOdds(activeBook, r.player.name, r.odds, dec);
        matched++;
      } else if(!r.matched && r.candidateName) {
        unmatched.push(r.candidateName);
      }
    }
    setParseResult({matched, unmatched:[...new Set(unmatched)].slice(0,15), book:activeBook, total:parsed.length});
    setPasteText("");
  }

  // Diff: book implied% - our adj%. Negative = we're shorter = value on our side.
  function diffVal(bookDec, adjProb){
    if(!bookDec||bookDec<=1) return null;
    return (1/bookDec) - adjProb;
  }
  const diffColor=(v)=>v===null?"var(--color-text-tertiary)":v>0.025?"#ef4444":v<-0.025?"#4ade80":"var(--color-text-secondary)";
  const fmtDiff=(v)=>v===null?"—":(v>0?"+":"")+(v*100).toFixed(1)+"%";

  // Players with any book odds entered (for active book)
  const withOdds = leaderMarket.filter(p=>getOdds(activeBook,p.name));
  const displayed = leaderMarket
    .filter(p=>filterTeam==="ALL"||p.team===filterTeam)
    .filter(p=>!search||p.name.toLowerCase().includes(search.toLowerCase()))
    .filter(p=>showAll||getOdds(activeBook,p.name));

  const edges = withOdds.filter(p=>{
    const o=getOdds(activeBook,p.name);
    return o && diffVal(o.dec,p.adjProb)>0.025;
  });
  const ourShort = withOdds.filter(p=>{
    const o=getOdds(activeBook,p.name);
    return o && diffVal(o.dec,p.adjProb)<-0.025;
  });

  const cs={padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11};

  return (
    <div>
      {/* Book selector + controls */}
      <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap",marginBottom:12}}>
        <div style={{display:"flex",borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)"}}>
          {[{id:"r1",label:"Round 1"},{id:"r2",label:"Round 2"},{id:"full",label:"Full Playoff"}].map(s=>(
            <button key={s.id} onClick={()=>setLScope(s.id)} style={{padding:"5px 12px",fontSize:11,border:"none",cursor:"pointer",
              background:lScope===s.id?"#3b82f6":"var(--color-background-secondary)",color:lScope===s.id?"white":"var(--color-text-secondary)"}}>
              {s.label}
            </button>
          ))}
        </div>
        <Seg options={STATS.map(s=>({id:s.id,label:s.label}))} value={lStat} onChange={setLStat} accent="#1d4ed8"/>
        <div style={{marginLeft:"auto",display:"flex",gap:6,alignItems:"center"}}>
          {BOOKS.map(b=>{
            const cnt=Object.keys(bookOdds[b]||{}).length;
            return <button key={b} onClick={()=>setActiveBook(b)} style={{
              padding:"4px 10px",fontSize:11,borderRadius:3,border:"0.5px solid",cursor:"pointer",
              borderColor:activeBook===b?"#3b82f6":"var(--color-border-secondary)",
              background:activeBook===b?"rgba(59,130,246,0.15)":"var(--color-background-secondary)",
              color:activeBook===b?"#60a5fa":"var(--color-text-secondary)",fontWeight:activeBook===b?500:400,
              position:"relative",
            }}>
              {b}
              {cnt>0&&<span style={{marginLeft:5,fontSize:9,padding:"1px 4px",borderRadius:8,
                background:activeBook===b?"#3b82f6":"var(--color-border-secondary)",
                color:"white"}}>{cnt}</span>}
            </button>;
          })}
        </div>
      </div>

      {/* Paste panel */}
      <Card style={{marginBottom:14}}>
        <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
          <SH title={`Paste ${activeBook} Odds`} sub="Copy the odds table from the book page and paste below — player names + American odds parsed automatically"/>
          <button onClick={()=>setShowPastePanel(v=>!v)} style={{marginLeft:"auto",padding:"3px 10px",fontSize:11,
            borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",
            border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>
            {showPastePanel?"Hide":"Show"}
          </button>
        </div>
        {showPastePanel&&<>
          <textarea value={pasteText} onChange={e=>setPasteText(e.target.value)}
            placeholder={`Paste ${activeBook} odds here…\n\nExample format (any of these work):\n  Bryan Rust +390\n  Evgeni Malkin +600\n  Sidney Crosby +650\n\nThe parser handles extra text, headers, and whitespace automatically.`}
            style={{width:"100%",height:140,fontSize:11,fontFamily:"var(--font-mono)",
              background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
              borderRadius:"var(--border-radius-md)",padding:10,color:"var(--color-text-primary)",
              resize:"vertical",boxSizing:"border-box",marginBottom:8}}/>
          <div style={{display:"flex",gap:8,alignItems:"center"}}>
            <button onClick={handleParse} disabled={!pasteText.trim()} style={{
              padding:"6px 18px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
              cursor:pasteText.trim()?"pointer":"default",
              background:pasteText.trim()?"#3b82f6":"var(--color-background-secondary)",
              color:pasteText.trim()?"white":"var(--color-text-tertiary)"}}>
              Parse & Import
            </button>
            <button onClick={()=>{clearBook(activeBook);setParseResult(null);}} style={{
              padding:"5px 12px",fontSize:11,borderRadius:"var(--border-radius-md)",
              background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
              color:"var(--color-text-secondary)",cursor:"pointer"}}>
              Clear {activeBook}
            </button>
            {!leaderMarket.length&&<span style={{fontSize:11,color:"var(--color-text-tertiary)"}}>Note: load player data for name matching</span>}
          </div>
          {parseResult&&<div style={{marginTop:10,padding:"8px 12px",borderRadius:"var(--border-radius-md)",
            background:parseResult.matched>0?"rgba(16,185,129,0.08)":"rgba(239,68,68,0.08)",
            border:`0.5px solid ${parseResult.matched>0?"rgba(16,185,129,0.3)":"rgba(239,68,68,0.3)"}`}}>
            <div style={{fontSize:11,fontWeight:500,color:parseResult.matched>0?"#10b981":"#ef4444",marginBottom:parseResult.unmatched.length?4:0}}>
              ✓ {parseResult.matched} players imported into {parseResult.book}
            </div>
            {parseResult.unmatched.length>0&&<div style={{fontSize:10,color:"var(--color-text-secondary)"}}>
              Could not match: {parseResult.unmatched.join(", ")}
              {parseResult.unmatched.length>=15&&"…"}
              <div style={{marginTop:2,color:"var(--color-text-tertiary)"}}>
                Check spelling or add manually in the table below.
              </div>
            </div>}
          </div>}
        </>}
      </Card>

      {/* Edge summary */}
      {withOdds.length>0&&<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:14}}>
        <Card style={{background:dark?"rgba(239,68,68,0.07)":"rgba(239,68,68,0.04)",border:"0.5px solid rgba(239,68,68,0.2)"}}>
          <div style={{fontSize:9,fontWeight:500,textTransform:"uppercase",letterSpacing:"0.08em",color:"#ef4444",marginBottom:6}}>
            Book shorter than us (&gt;2.5%) — {edges.length}
          </div>
          {edges.length===0
            ?<div style={{fontSize:10,color:"var(--color-text-tertiary)"}}>None</div>
            :edges.map(p=>{const o=getOdds(activeBook,p.name);const v=diffVal(o?.dec,p.adjProb);return(
              <div key={p.name} style={{display:"flex",justifyContent:"space-between",padding:"2px 0",borderBottom:"0.5px solid var(--color-border-tertiary)",fontSize:10}}>
                <span style={{fontWeight:500}}>{p.name} <span style={{color:"var(--color-text-tertiary)",fontWeight:400,fontSize:9}}>({p.team})</span></span>
                <span style={{fontFamily:"var(--font-mono)",color:"#ef4444"}}>{o?.amer>0?`+${o.amer}`:o?.amer} · {fmtDiff(v)}</span>
              </div>
            );})}
        </Card>
        <Card style={{background:dark?"rgba(74,222,128,0.07)":"rgba(74,222,128,0.04)",border:"0.5px solid rgba(74,222,128,0.2)"}}>
          <div style={{fontSize:9,fontWeight:500,textTransform:"uppercase",letterSpacing:"0.08em",color:"#4ade80",marginBottom:6}}>
            We're shorter than book (&gt;2.5%) — {ourShort.length}
          </div>
          {ourShort.length===0
            ?<div style={{fontSize:10,color:"var(--color-text-tertiary)"}}>None</div>
            :ourShort.map(p=>{const o=getOdds(activeBook,p.name);const v=diffVal(o?.dec,p.adjProb);return(
              <div key={p.name} style={{display:"flex",justifyContent:"space-between",padding:"2px 0",borderBottom:"0.5px solid var(--color-border-tertiary)",fontSize:10}}>
                <span style={{fontWeight:500}}>{p.name} <span style={{color:"var(--color-text-tertiary)",fontWeight:400,fontSize:9}}>({p.team})</span></span>
                <span style={{fontFamily:"var(--font-mono)",color:"#4ade80"}}>{o?.amer>0?`+${o.amer}`:o?.amer} · {fmtDiff(v)}</span>
              </div>
            );})}
        </Card>
      </div>}

      {/* Comparison table */}
      <Card>
        <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:10,flexWrap:"wrap"}}>
          <SH title={`${lScope==="r1"?"R1":lScope==="r2"?"R2":"Playoff"} ${STATS.find(s=>s.id===lStat)?.label||""} — ${activeBook} vs Our Price`}
            sub={withOdds.length?`${withOdds.length} players with ${activeBook} odds · ${displayed.length} shown`:"Paste odds above to populate"}/>
          <div style={{marginLeft:"auto",display:"flex",gap:8,alignItems:"center"}}>
            <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)} style={SEL}>
              <option value="ALL">All Teams</option>
              {teams.map(t=><option key={t} value={t}>{t}</option>)}
            </select>
            <input placeholder="Search…" value={search} onChange={e=>setSearch(e.target.value)}
              style={{padding:"4px 8px",fontSize:11,background:"var(--color-background-secondary)",
                border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",
                color:"var(--color-text-primary)",width:130}}/>
            <Toggle label="All players" checked={showAll} onChange={setShowAll}/>
          </div>
        </div>

        {!leaderMarket.length
          ?<div style={{fontSize:12,color:"var(--color-text-secondary)",padding:"20px 0",textAlign:"center"}}>
            Configure R1 Matchups and load player data first.
          </div>
          :<div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><tr style={{borderBottom:"0.5px solid var(--color-border-secondary)"}}>
              {["#","Player","Team","λ","Our%","Our Odds",activeBook+" Amer",activeBook+" Dec","Diff",
                ...BOOKS.filter(b=>b!==activeBook).map(b=>b.slice(0,3))
              ].map((h,i)=>(
                <th key={i} style={{padding:"5px 8px",textAlign:i<2?"left":"right",
                  color:i>=9?"var(--color-text-tertiary)":"var(--color-text-secondary)",
                  fontWeight:i>=9?400:500,fontSize:i>=9?9:10,textTransform:"uppercase"}}>{h}</th>
              ))}
            </tr></thead>
            <tbody>{displayed.map((p,i)=>{
              const ourAmer=toAmer(p.adjProb);
              const ourDec=toDec(p.adjProb);
              const bo=getOdds(activeBook,p.name);
              const dv=bo?diffVal(bo.dec,p.adjProb):null;
              const isEdge=dv!==null&&Math.abs(dv)>0.025;
              return (
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",
                  background:isEdge?(dv>0?(dark?"rgba(239,68,68,0.06)":"rgba(239,68,68,0.03)"):(dark?"rgba(74,222,128,0.06)":"rgba(74,222,128,0.03)")):
                    i%2===0?"transparent":(dark?"rgba(255,255,255,0.015)":"rgba(0,0,0,0.01)")}}>
                  <td style={{padding:"3px 8px",color:"var(--color-text-tertiary)",fontSize:10}}>{leaderMarket.indexOf(p)+1}</td>
                  <td style={{padding:"3px 8px",fontWeight:bo?500:400}}>{p.name}</td>
                  <td style={{...cs}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
                  <td style={{...cs,color:"var(--color-text-secondary)"}}>{p.lambda.toFixed(2)}</td>
                  <td style={{...cs}}>{(p.adjProb*100).toFixed(2)}%</td>
                  <td style={{...cs,fontWeight:500,color:ourAmer<0?"#4ade80":"var(--color-text-primary)"}}>
                    {ourAmer>0?`+${ourAmer}`:ourAmer}
                    <span style={{color:"var(--color-text-tertiary)",fontWeight:400,fontSize:9,marginLeft:4}}>({ourDec.toFixed(2)})</span>
                  </td>
                  {/* Active book odds — editable */}
                  <td style={{padding:"2px 4px",textAlign:"right"}}>
                    <input type="number" step={1} value={bo?.amer??""} placeholder="—"
                      onChange={e=>{const a=parseInt(e.target.value)||0;if(Math.abs(a)>=100)setPlayerOdds(activeBook,p.name,a,amerToDecimal(a));}}
                      style={{width:58,fontSize:11,textAlign:"center",padding:"2px 4px",fontFamily:"var(--font-mono)",
                        background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
                        borderRadius:3,color:"var(--color-text-primary)"}}/>
                  </td>
                  <td style={{...cs,color:bo?"var(--color-text-secondary)":"var(--color-text-tertiary)"}}>
                    {bo?bo.dec.toFixed(2):"—"}
                  </td>
                  <td style={{...cs,fontWeight:isEdge?500:400,color:diffColor(dv)}}>{fmtDiff(dv)}</td>
                  {/* Other books — compact */}
                  {BOOKS.filter(b=>b!==activeBook).map(b=>{
                    const o=getOdds(b,p.name);
                    const dv2=o?diffVal(o.dec,p.adjProb):null;
                    return <td key={b} style={{padding:"2px 3px",textAlign:"right"}}>
                      <input type="number" step={1} value={o?.amer??""} placeholder="—"
                        onChange={e=>{const a=parseInt(e.target.value)||0;if(Math.abs(a)>=100)setPlayerOdds(b,p.name,a,amerToDecimal(a));}}
                        style={{width:48,fontSize:10,textAlign:"center",padding:"1px 3px",fontFamily:"var(--font-mono)",
                          background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-tertiary)",
                          borderRadius:3,color:dv2!==null?diffColor(dv2):"var(--color-text-tertiary)"}}/>
                    </td>;
                  })}
                </tr>
              );
            })}</tbody>
          </table>
        </div>}
        <div style={{marginTop:8,padding:"5px 10px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",fontSize:10,color:"var(--color-text-tertiary)"}}>
          Diff = Book implied% − Our adj%. <span style={{color:"#ef4444"}}>Red = book shorter (tighter than us)</span> · <span style={{color:"#4ade80"}}>Green = we're shorter (tighter than book)</span>. Threshold ±2.5%. Manual edits accepted in American odds columns.
        </div>
      </Card>
    </div>
  );
}

// ─── EXPORT / IMPORT PANEL ───────────────────────────────────────────────────
function ExportImportPanel({exportState,importState}) {
  const [exportJson,setExportJson]=useState("");
  const [importText,setImportText]=useState("");
  const [msg,setMsg]=useState("");
  const [copied,setCopied]=useState(false);

  function doExport(){
    const json=exportState();
    setExportJson(json);
    // Try clipboard
    if(navigator.clipboard){
      navigator.clipboard.writeText(json).then(()=>{setCopied(true);setTimeout(()=>setCopied(false),2000);}).catch(()=>{});
    }
  }
  function doImport(){
    const r=importState(importText);
    setMsg(r.ok?"✓ State restored successfully":`Error: ${r.error}`);
    if(r.ok){setImportText("");setExportJson("");}
  }

  return <div>
    <div style={{display:"flex",gap:8,marginBottom:8,alignItems:"center"}}>
      <button onClick={doExport} style={{padding:"5px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",
        background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
        color:"var(--color-text-primary)",cursor:"pointer"}}>
        {copied?"✓ Copied!":"Export State"}
      </button>
      <span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>Copies JSON to clipboard + shows below</span>
    </div>
    {exportJson&&<textarea readOnly value={exportJson} onClick={e=>e.target.select()}
      style={{width:"100%",height:80,fontSize:9,fontFamily:"var(--font-mono)",
        background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
        borderRadius:"var(--border-radius-md)",padding:8,color:"var(--color-text-secondary)",
        resize:"vertical",boxSizing:"border-box",marginBottom:8}}/>}
    <textarea value={importText} onChange={e=>setImportText(e.target.value)}
      placeholder="Paste exported JSON here to restore state…"
      style={{width:"100%",height:60,fontSize:9,fontFamily:"var(--font-mono)",
        background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
        borderRadius:"var(--border-radius-md)",padding:8,color:"var(--color-text-primary)",
        resize:"vertical",boxSizing:"border-box",marginBottom:6}}/>
    <button onClick={doImport} disabled={!importText.trim()} style={{
      padding:"4px 12px",fontSize:11,borderRadius:"var(--border-radius-md)",border:"none",
      cursor:importText.trim()?"pointer":"default",
      background:importText.trim()?"#1d4ed8":"var(--color-background-secondary)",
      color:importText.trim()?"white":"var(--color-text-tertiary)"}}>
      Restore from JSON
    </button>
    {msg&&<div style={{marginTop:6,fontSize:11,color:msg.startsWith("✓")?"#10b981":"#ef4444"}}>{msg}</div>}
  </div>;
}

// ─── GAME STAT IMPORTER ──────────────────────────────────────────────────────
// v28: full rewrite around parseHRFullPage.
// - Single paste: entire HR game page (header + scoring + both teams + advanced + goalies)
// - Auto-detects series from team abbrs in the paste
// - Auto-detects game# = max played gameNum for that series + 1 (with override)
// - Imports BOTH teams' skater stats + goalie stats + game result (score, OT) in one shot
// - Shows warnings for unmatched players (late callups not in skaters.csv)
// - Per-game dedup via paste hash + (seriesIdx, gameNum) key
// - Undo restores both teams' player + goalie state and clears the game result
function GameStatImporter({players,setPlayers,goalies,setGoalies,allSeries,setAllSeries,onGameUploaded}) {
  const [paste,setPaste]=useState("");
  const [preview,setPreview]=useState(null); // parsed preview before commit
  const [overrideGame,setOverrideGame]=useState(null); // null = use auto-detected
  const [overrideSeries,setOverrideSeries]=useState(null); // null = use auto-detected
  const [unmatchedDecisions,setUnmatchedDecisions]=useState({}); // name -> "skip"|"add"
  const [result,setResult]=useState(null);
  const [err,setErr]=useState("");
  // v78: format selector. "auto" detects from paste; "hr" forces Hockey Reference; "nst" forces Natural Stat Trick.
  const [format,setFormat]=useState("auto");

  // Imports log persisted to localStorage. Each entry covers BOTH teams of one game.
  // Schema: {id, ts, seriesIdx, seriesLabel, round, game, hash, awayAbbr, homeAbbr,
  //          awayScore, homeScore, ot, matched, unmatched, deltaPlayers, deltaGoalies}
  const [imports,setImports] = useState(()=>{try{const s=localStorage.getItem("nhl_imports_v28");return s?JSON.parse(s):[];}catch{return[];}});
  useEffect(()=>{try{localStorage.setItem("nhl_imports_v28",JSON.stringify(imports));}catch{}},[imports]);

  function hashStr(str){let h=0;for(let i=0;i<str.length;i++){h=((h<<5)-h)+str.charCodeAt(i);h|=0;}return (h>>>0).toString(36);}
  // v28.1: NFD-normalize then strip combining diacritics (ö → o, č → c, ü → u, etc.)
  // Critical for matching HR-style names with accents against ASCII-stripped CSV names (or vice versa).
  const norm=s=>(s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/[^a-z0-9]/g,"");

  // Auto-detect series from team abbrs in parsed result
  function detectSeriesIdx(awayAbbr, homeAbbr){
    if (!allSeries) return -1;
    const teams = new Set([awayAbbr, homeAbbr]);
    return allSeries.findIndex(sr => teams.has(sr.homeAbbr) && teams.has(sr.awayAbbr));
  }

  // Auto-detect next game# = max gameNum with a recorded result + 1
  function detectGameNum(seriesIdx){
    const sr = allSeries?.[seriesIdx];
    if (!sr || !sr.games) return 1;
    let maxPlayed = 0;
    sr.games.forEach((g, idx) => {
      if (g && g.result) maxPlayed = Math.max(maxPlayed, idx+1);
    });
    return Math.min(7, maxPlayed + 1);
  }

  // Step 1: parse + show preview (no commit yet)
  function handleParse(){
    setErr(""); setResult(null); setPreview(null); setUnmatchedDecisions({});
    if(!players){setErr("Load skaters.csv first.");return;}
    if(!paste.trim()) return;

    // v78: select parser based on format selector. "auto" sniffs the paste for NST signature
    // v84: auto-detect: NST has " - Individual" headers; ESPN has "forwards" + "defensemen" + "G A +/- S SM BS";
    // HR is the fallback.
    let r;
    let detectedFormat = format;
    if (format === "auto") {
      if (/ - Individual\s*$/m.test(paste)) detectedFormat = "nst";
      // ESPN auto-detect: has "forwards" + "defensemen" sections. Stat header may be vertical
      // (each column on its own line: "G", "A", "+/-"...) or single-line; either way the presence
      // of "forwards" + "defensemen" + "FO%" (last column) is a strong signature.
      else if (/^forwards\s*$/im.test(paste) && /^defensemen\s*$/im.test(paste) && /^FO%\s*$/m.test(paste)) detectedFormat = "espn";
      else detectedFormat = "hr";
    }
    if (detectedFormat === "nst") {
      r = parseNSTGameReport(paste);
    } else if (detectedFormat === "espn") {
      r = parseESPNBoxScore(paste);
    } else {
      r = parseHRFullPage(paste);
    }
    if (r.error) { setErr(r.error); return; }

    const detectedSeries = detectSeriesIdx(r.awayAbbr, r.homeAbbr);
    if (detectedSeries === -1) {
      setErr(`Parsed teams ${r.awayAbbr} @ ${r.homeAbbr} — no matching series in setup. Add this matchup in Series Pricer first.`);
      return;
    }
    const detectedGame = detectGameNum(detectedSeries);

    // Build matched/unmatched lists for both teams against players roster
    const checkSide = (sidePlayers, teamAbbr) => {
      const matched = []; const unmatched = [];
      sidePlayers.forEach(u => {
        // Skip the goalie row that often sneaks into HR skater table (TOI ~60min, no shots/G/A)
        // We detect that by also checking goalies list
        const m = players.find(p => p.team===teamAbbr && (norm(u.name)===norm(p.name)
          || (norm(u.name).length>=4 && norm(p.name).endsWith(norm(u.name.split(" ").pop()||"")))));
        if (m) matched.push({u, p:m});
        else unmatched.push(u);
      });
      return {matched, unmatched};
    };
    const away = checkSide(r.awayPlayers, r.awayAbbr);
    const home = checkSide(r.homePlayers, r.homeAbbr);

    // Filter out goalies from "unmatched" (they're in goalies.csv, not skaters.csv)
    const goalieNames = new Set([
      ...r.awayGoalies.map(g=>norm(g.name)),
      ...r.homeGoalies.map(g=>norm(g.name)),
    ]);
    away.unmatched = away.unmatched.filter(u => !goalieNames.has(norm(u.name)));
    home.unmatched = home.unmatched.filter(u => !goalieNames.has(norm(u.name)));

    // Goalie matching
    const checkGoalies = (sideGoalies, teamAbbr) => {
      const matched = []; const unmatched = [];
      sideGoalies.forEach(u => {
        const m = (goalies||[]).find(g => g.team===teamAbbr && (norm(u.name)===norm(g.name)
          || (norm(u.name).length>=4 && norm(g.name).endsWith(norm(u.name.split(" ").pop()||"")))));
        if (m) matched.push({u, g:m});
        else unmatched.push(u);
      });
      return {matched, unmatched};
    };
    const awayG = checkGoalies(r.awayGoalies, r.awayAbbr);
    const homeG = checkGoalies(r.homeGoalies, r.homeAbbr);

    setPreview({
      ...r,
      detectedSeries, detectedGame,
      awayMatched: away.matched, awayUnmatched: away.unmatched,
      homeMatched: home.matched, homeUnmatched: home.unmatched,
      awayGoaliesMatched: awayG.matched, awayGoaliesUnmatched: awayG.unmatched,
      homeGoaliesMatched: homeG.matched, homeGoaliesUnmatched: homeG.unmatched,
    });
    setOverrideGame(null);
    setOverrideSeries(null);
  }

  // Step 2: commit the import after user reviews
  function handleCommit(){
    if (!preview) return;
    setErr(""); setResult(null);

    const seriesIdx = overrideSeries != null ? overrideSeries : preview.detectedSeries;
    const gameNum = overrideGame != null ? overrideGame : preview.detectedGame;
    const sr = allSeries?.[seriesIdx];
    if (!sr) { setErr("Series not found."); return; }

    const hash = hashStr(paste.trim());
    const dup = imports.find(x => x.seriesIdx===seriesIdx && x.game===gameNum);
    if (dup) {
      setErr(`G${gameNum} already imported for ${sr.homeAbbr} vs ${sr.awayAbbr} at ${dup.ts}. Undo it first to re-import.`);
      return;
    }

    // Build skater pGames updates
    const deltaPlayers = []; // {name, team, round, game}
    const playersById = new Map((players||[]).map(p=>[p.name+"|"+p.team, p]));

    const applySide = (sideMatched, teamAbbr) => {
      sideMatched.forEach(({u, p}) => {
        const base = migratePlayer(p);
        const existingGames = base.pGames || [];
        const already = existingGames.some(e => e.round===1 && e.game===gameNum);
        if (already) return;
        const newEntry = {
          round: 1, game: gameNum,
          g: u.g||0, a: u.a||0, sog: u.sog||0,
          hit: u.hit||0, blk: u.blk||0, tk: 0, pim: u.pim||0, give: 0,
          toi: u.toi||0,
          _source: "hr_full_v28",
        };
        const updated = withRollups({...base, pGames:[...existingGames, newEntry]});
        playersById.set(p.name+"|"+p.team, updated);
        deltaPlayers.push({name:p.name, team:p.team, round:1, game:gameNum});
      });
    };
    applySide(preview.awayMatched, preview.awayAbbr);
    applySide(preview.homeMatched, preview.homeAbbr);

    // Handle unmatched: add as new players if user chose "add"
    const unmatchedAdded = [];
    const handleUnmatchedSide = (sideUnmatched, teamAbbr) => {
      sideUnmatched.forEach(u => {
        const decision = unmatchedDecisions[u.name+"|"+teamAbbr];
        if (decision === "add") {
          // Synthesize a player record with default rates (zero baseline)
          const newP = {
            name: u.name, team: teamAbbr, position: "F",
            gp: 1, g_pg: 0, a_pg: 0, sog_pg: 0, hit_pg: 0, blk_pg: 0,
            take_pg: 0, pim_pg: 0, give_pg: 0,
            pG: 0, pA: 0, pSOG: 0, pHIT: 0, pBLK: 0, pTK: 0, pPIM: 0, pGIVE: 0, pGP: 0,
            lineRole: "MID6",
            _addedViaImport: true,
            pGames: [{
              round: 1, game: gameNum,
              g: u.g||0, a: u.a||0, sog: u.sog||0,
              hit: u.hit||0, blk: u.blk||0, tk: 0, pim: u.pim||0, give: 0,
              toi: u.toi||0, _source: "hr_full_v28",
            }],
          };
          const rolled = withRollups(newP);
          playersById.set(rolled.name+"|"+rolled.team, rolled);
          deltaPlayers.push({name:rolled.name, team:rolled.team, round:1, game:gameNum, _added:true});
          unmatchedAdded.push(rolled.name);
        }
      });
    };
    handleUnmatchedSide(preview.awayUnmatched, preview.awayAbbr);
    handleUnmatchedSide(preview.homeUnmatched, preview.homeAbbr);

    // Goalie updates
    const deltaGoalies = [];
    const goaliesById = new Map((goalies||[]).map(g=>[g.name+"|"+g.team, g]));
    const applyGoalies = (sideMatched) => {
      sideMatched.forEach(({u, g}) => {
        const existing = g.pGames || [];
        const already = existing.some(e => e.round===1 && e.game===gameNum);
        if (already) return;
        const newEntry = {
          round:1, game:gameNum, ga:u.ga||0, sa:u.sa||0, sv:u.sv||0,
          so:u.so||0, toi:u.toi||0, dec:u.dec||"",
        };
        const rolled = withGoalieRollups({...g, pGames:[...existing, newEntry]});
        goaliesById.set(g.name+"|"+g.team, rolled);
        deltaGoalies.push({name:g.name, team:g.team, round:1, game:gameNum});
      });
    };
    applyGoalies(preview.awayGoaliesMatched);
    applyGoalies(preview.homeGoaliesMatched);

    // Push state
    const newPlayers = [...playersById.values()];
    setPlayers(newPlayers);
    if (setGoalies && goalies) setGoalies([...goaliesById.values()]);

    // Push score + OT into Series Pricer game row.
    // games[gameNum-1] convention: home/away are determined by the series record's homeAbbr/awayAbbr,
    // NOT by HR's away/home. So map preview's away/home → series's home/away.
    if (setAllSeries) {
      setAllSeries(prev => {
        const u = [...prev];
        const s2 = u[seriesIdx];
        const games = [...s2.games];
        const idx = gameNum - 1;
        // Resolve which score corresponds to series.homeAbbr
        const homeIsAway = preview.awayAbbr === s2.homeAbbr;
        const homeScore = homeIsAway ? preview.awayScore : preview.homeScore;
        const awayScore = homeIsAway ? preview.homeScore : preview.awayScore;
        const winResult = homeScore > awayScore ? "home" : awayScore > homeScore ? "away" : null;
        games[idx] = {
          ...games[idx],
          homeScore, awayScore,
          result: winResult || games[idx].result,
          wentOT: !!preview.ot,
          ot: !!preview.ot,  // legacy alias kept for compat
          // v89: persist OT scorer if present in the parsed paste.
          // Resolve to canonical roster name if possible — parser may have spelling that differs slightly.
          otScorer: preview.otScorer || null,
        };
        u[seriesIdx] = {...s2, games};
        return u;
      });
    }

    // Log it
    const matchedCount = preview.awayMatched.length + preview.homeMatched.length;
    const unmatchedAll = [...preview.awayUnmatched, ...preview.homeUnmatched].map(u=>u.name);
    const seriesLabel = `${sr.homeAbbr} vs ${sr.awayAbbr}`;
    const entry = {
      id: Date.now()+"-"+Math.random().toString(36).slice(2,7),
      ts: new Date().toLocaleString(),
      seriesIdx, seriesLabel, round:1, game:gameNum, hash,
      awayAbbr: preview.awayAbbr, homeAbbr: preview.homeAbbr,
      awayScore: preview.awayScore, homeScore: preview.homeScore,
      ot: !!preview.ot,
      matched: matchedCount, unmatched: unmatchedAll.slice(0,20),
      addedPlayers: unmatchedAdded,
      deltaPlayers, deltaGoalies,
    };
    setImports(prev => [entry, ...prev].slice(0,200));
    setResult({
      seriesLabel, gameNum, awayAbbr:preview.awayAbbr, homeAbbr:preview.homeAbbr,
      awayScore:preview.awayScore, homeScore:preview.homeScore, ot:!!preview.ot,
      matched: matchedCount, unmatched: unmatchedAll.length,
      addedPlayers: unmatchedAdded, goalies: deltaGoalies.length,
    });
    setPaste(""); setPreview(null);
    // v31: signal SeriesTab to auto-run the unified sim with the new state
    if (onGameUploaded) onGameUploaded();
  }

  function handleUndo(entryId){
    const entry = imports.find(x => x.id===entryId);
    if (!entry) return;
    const round = entry.round||1, game = entry.game;

    // Roll back skater pGames
    const playerKeys = new Set((entry.deltaPlayers||[]).map(d => d.name+"|"+d.team));
    const addedKeys = new Set((entry.deltaPlayers||[]).filter(d=>d._added).map(d=>d.name+"|"+d.team));
    setPlayers(prev => prev
      .map(p => {
        if (!playerKeys.has(p.name+"|"+p.team)) return p;
        if (!p.pGames) return p;
        const filtered = p.pGames.filter(e => !(e.round===round && e.game===game));
        if (filtered.length === p.pGames.length) return p;
        return withRollups({...p, pGames:filtered});
      })
      .filter(p => !(addedKeys.has(p.name+"|"+p.team) && (!p.pGames || p.pGames.length===0)))
    );

    // Roll back goalie pGames
    const goalieKeys = new Set((entry.deltaGoalies||[]).map(d => d.name+"|"+d.team));
    if (setGoalies && goalies) {
      setGoalies(prev => prev.map(g => {
        if (!goalieKeys.has(g.name+"|"+g.team)) return g;
        if (!g.pGames) return g;
        const filtered = g.pGames.filter(e => !(e.round===round && e.game===game));
        if (filtered.length === g.pGames.length) return g;
        return withGoalieRollups({...g, pGames:filtered});
      }));
    }

    // Clear game result
    if (setAllSeries) {
      setAllSeries(prev => {
        const u = [...prev];
        if (!u[entry.seriesIdx]) return prev;
        const games = [...u[entry.seriesIdx].games];
        games[entry.game-1] = {...games[entry.game-1], homeScore:null, awayScore:null, result:null, ot:false};
        u[entry.seriesIdx] = {...u[entry.seriesIdx], games};
        return u;
      });
    }
    setImports(prev => prev.filter(x => x.id !== entryId));
  }

  // Status grid: show all 7 games for currently-detected (or first) series
  const focusSeriesIdx = preview ? (overrideSeries != null ? overrideSeries : preview.detectedSeries) : 0;
  const focusSeries = allSeries?.[focusSeriesIdx];
  const seriesImports = imports.filter(x => x.seriesIdx === focusSeriesIdx);

  return <div>
    <div style={{fontSize:10,color:"var(--color-text-tertiary)",marginBottom:8,lineHeight:1.5}}>
      Paste an <strong>entire</strong> Hockey Reference, Natural Stat Trick, or ESPN box score
      page (Ctrl+A → Ctrl+C). Series + game # auto-detected. One paste covers both teams.
    </div>

    {/* v78: format selector */}
    <div style={{display:"flex",gap:0,marginBottom:8,borderRadius:"var(--border-radius-md)",overflow:"hidden",
      border:"0.5px solid var(--color-border-secondary)",width:"fit-content"}}>
      {[{id:"auto",l:"Auto-detect"},{id:"hr",l:"Hockey Reference"},{id:"nst",l:"Natural Stat Trick"},{id:"espn",l:"ESPN"}].map(f => (
        <button key={f.id} onClick={()=>setFormat(f.id)}
          style={{padding:"5px 14px",fontSize:11,border:"none",
            borderRight:"0.5px solid var(--color-border-tertiary)",cursor:"pointer",
            background:format===f.id?"#3b82f6":"var(--color-background-secondary)",
            color:format===f.id?"white":"var(--color-text-secondary)",fontWeight:format===f.id?500:400}}>
          {f.l}
        </button>
      ))}
    </div>

    <textarea value={paste} onChange={e=>{setPaste(e.target.value); setPreview(null); setResult(null); setErr("");}}
      placeholder={"Paste HR or NST game page here…"}
      style={{width:"100%",height:140,fontSize:10,fontFamily:"var(--font-mono)",
        background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
        borderRadius:"var(--border-radius-md)",padding:10,color:"var(--color-text-primary)",
        resize:"vertical",boxSizing:"border-box",marginBottom:8}}/>

    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:8}}>
      <button onClick={handleParse} disabled={!paste.trim()||!players}
        style={{padding:"6px 18px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
          cursor:paste.trim()&&players?"pointer":"default",
          background:paste.trim()&&players?"#3b82f6":"var(--color-background-secondary)",
          color:paste.trim()&&players?"white":"var(--color-text-tertiary)"}}>
        1. Parse Preview
      </button>
      <button onClick={()=>{setPaste("");setPreview(null);setResult(null);setErr("");}}
        style={{padding:"5px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",
          background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
          color:"var(--color-text-secondary)",cursor:"pointer"}}>Clear</button>
      {!players&&<span style={{fontSize:11,color:"#f59e0b"}}>Load skaters.csv first</span>}
      {!goalies&&<span style={{fontSize:11,color:"#f59e0b"}}>Load goalies.csv to capture goalie stats</span>}
    </div>

    {err&&<div style={{marginBottom:8,padding:8,borderRadius:"var(--border-radius-md)",
      background:"rgba(239,68,68,0.1)",border:"0.5px solid rgba(239,68,68,0.3)",fontSize:11,color:"#ef4444"}}>{err}</div>}

    {/* PREVIEW PANEL */}
    {preview && <div style={{marginBottom:10,padding:10,borderRadius:"var(--border-radius-md)",
      background:"rgba(59,130,246,0.06)",border:"0.5px solid rgba(59,130,246,0.3)"}}>
      <div style={{fontSize:11,fontWeight:500,color:"#3b82f6",marginBottom:8}}>
        Parsed: {preview.awayAbbr} {preview.awayScore} @ {preview.homeAbbr} {preview.homeScore}
        {preview.ot && <span style={{marginLeft:6,color:"#f59e0b"}}>(OT)</span>}
        {preview.dateISO && <span style={{marginLeft:6,color:"var(--color-text-tertiary)",fontWeight:400}}>· {preview.dateISO}</span>}
      </div>

      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:8}}>
        <div>
          <div style={{fontSize:9,color:"var(--color-text-secondary)",marginBottom:3}}>SERIES (auto-detected)</div>
          <select value={overrideSeries != null ? overrideSeries : preview.detectedSeries}
            onChange={e=>setOverrideSeries(+e.target.value)}
            style={{width:"100%",padding:"4px 8px",fontSize:11,background:"var(--color-background-secondary)",
              border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)"}}>
            {(allSeries||[]).map((sr,i)=>(
              <option key={i} value={i}>{sr.homeAbbr&&sr.awayAbbr?`${sr.homeAbbr} vs ${sr.awayAbbr}`:`Series ${i+1}`}{i===preview.detectedSeries?" ✓":""}</option>
            ))}
          </select>
        </div>
        <div>
          <div style={{fontSize:9,color:"var(--color-text-secondary)",marginBottom:3}}>GAME # (auto: G{preview.detectedGame})</div>
          <select value={overrideGame != null ? overrideGame : preview.detectedGame}
            onChange={e=>setOverrideGame(+e.target.value)}
            style={{width:"100%",padding:"4px 8px",fontSize:11,background:"var(--color-background-secondary)",
              border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)"}}>
            {[1,2,3,4,5,6,7].map(n=><option key={n} value={n}>Game {n}{n===preview.detectedGame?" ✓":""}</option>)}
          </select>
        </div>
      </div>

      {/* Match summary */}
      <div style={{fontSize:10,marginBottom:6,color:"var(--color-text-secondary)"}}>
        <span style={{color:"#10b981"}}>{preview.awayMatched.length}</span> {preview.awayAbbr} matched
        {preview.awayUnmatched.length>0&&<span style={{color:"#f59e0b"}}> · {preview.awayUnmatched.length} unmatched</span>}
        {" · "}
        <span style={{color:"#10b981"}}>{preview.homeMatched.length}</span> {preview.homeAbbr} matched
        {preview.homeUnmatched.length>0&&<span style={{color:"#f59e0b"}}> · {preview.homeUnmatched.length} unmatched</span>}
        {" · Goalies: "}
        <span style={{color:"#10b981"}}>{preview.awayGoaliesMatched.length+preview.homeGoaliesMatched.length}</span> matched
        {(preview.awayGoaliesUnmatched.length+preview.homeGoaliesUnmatched.length)>0
          &&<span style={{color:"#f59e0b"}}> · {preview.awayGoaliesUnmatched.length+preview.homeGoaliesUnmatched.length} unmatched</span>}
      </div>

      {/* Unmatched player decisions */}
      {[...preview.awayUnmatched.map(u=>({u,team:preview.awayAbbr})),
        ...preview.homeUnmatched.map(u=>({u,team:preview.homeAbbr}))].length>0 && (
        <div style={{marginBottom:8,padding:8,background:"rgba(245,158,11,0.08)",borderRadius:"var(--border-radius-md)",border:"0.5px solid rgba(245,158,11,0.25)"}}>
          <div style={{fontSize:10,fontWeight:500,color:"#f59e0b",marginBottom:4}}>Unmatched skaters (not in roster)</div>
          {[...preview.awayUnmatched.map(u=>({u,team:preview.awayAbbr})),
            ...preview.homeUnmatched.map(u=>({u,team:preview.homeAbbr}))].map(({u,team},i)=>{
            const key = u.name+"|"+team;
            const decision = unmatchedDecisions[key] || "skip";
            return <div key={i} style={{display:"flex",gap:8,alignItems:"center",fontSize:10,padding:"2px 0"}}>
              <span style={{minWidth:140}}>{u.name} <span style={{color:"var(--color-text-tertiary)"}}>({team})</span></span>
              <span style={{color:"var(--color-text-secondary)",fontFamily:"var(--font-mono)",minWidth:120}}>
                {u.g}G {u.a}A · {u.sog}S · {u.hit}H {u.blk}B
              </span>
              <select value={decision} onChange={e=>setUnmatchedDecisions(prev=>({...prev,[key]:e.target.value}))}
                style={{padding:"2px 6px",fontSize:10,background:"var(--color-background-secondary)",
                  border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}>
                <option value="skip">Skip</option>
                <option value="add">Add as new player</option>
              </select>
            </div>;
          })}
        </div>
      )}

      {/* v29.1: Unmatched goalies display — read-only (goalies always skip on commit since
          we don't synthesize goalie records from box scores; user should load goalies.csv
          or manually add a goalie entry if missing). */}
      {[...preview.awayGoaliesUnmatched.map(u=>({u,team:preview.awayAbbr})),
        ...preview.homeGoaliesUnmatched.map(u=>({u,team:preview.homeAbbr}))].length>0 && (
        <div style={{marginBottom:8,padding:8,background:"rgba(245,158,11,0.08)",borderRadius:"var(--border-radius-md)",border:"0.5px solid rgba(245,158,11,0.25)"}}>
          <div style={{fontSize:10,fontWeight:500,color:"#f59e0b",marginBottom:4}}>Unmatched goalies (not in goalies.csv) — will be skipped on commit</div>
          {[...preview.awayGoaliesUnmatched.map(u=>({u,team:preview.awayAbbr})),
            ...preview.homeGoaliesUnmatched.map(u=>({u,team:preview.homeAbbr}))].map(({u,team},i)=>(
            <div key={i} style={{display:"flex",gap:8,alignItems:"center",fontSize:10,padding:"2px 0"}}>
              <span style={{minWidth:140}}>{u.name} <span style={{color:"var(--color-text-tertiary)"}}>({team})</span></span>
              <span style={{color:"var(--color-text-secondary)",fontFamily:"var(--font-mono)",minWidth:160}}>
                {u.dec||"—"} · {u.ga}GA / {u.sa}SA · {u.sv}SV{u.so>0?` · ${u.so}SO`:""}
              </span>
            </div>
          ))}
        </div>
      )}

      <div style={{display:"flex",gap:8,alignItems:"center"}}>
        <button onClick={handleCommit}
          style={{padding:"6px 18px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
            cursor:"pointer",background:"#10b981",color:"white"}}>
          2. Commit Import
        </button>
        <button onClick={()=>{setPreview(null);setUnmatchedDecisions({});}}
          style={{padding:"5px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",
            background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
            color:"var(--color-text-secondary)",cursor:"pointer"}}>Cancel</button>
      </div>
    </div>}

    {result&&<div style={{marginBottom:10,padding:8,borderRadius:"var(--border-radius-md)",
      background:"rgba(16,185,129,0.1)",border:"0.5px solid rgba(16,185,129,0.3)",fontSize:11}}>
      <div style={{color:"#10b981",fontWeight:500,marginBottom:2}}>
        ✓ {result.seriesLabel} G{result.gameNum}: {result.awayAbbr} {result.awayScore} @ {result.homeAbbr} {result.homeScore}
        {result.ot && " (OT)"} — {result.matched} skaters · {result.goalies} goalies
      </div>
      {result.unmatched>0&&<div style={{fontSize:10,color:"#f59e0b"}}>
        {result.unmatched} unmatched player{result.unmatched===1?"":"s"} {result.addedPlayers.length>0?`(${result.addedPlayers.length} added: ${result.addedPlayers.join(", ")})`:"(skipped)"}
      </div>}
    </div>}

    {/* Status grid for the focused series */}
    {focusSeries&&focusSeries.homeAbbr&&focusSeries.awayAbbr&&<div style={{marginTop:12}}>
      <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:6}}>
        {focusSeries.homeAbbr} vs {focusSeries.awayAbbr} — Game Status
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(7,1fr)",gap:4}}>
        {(()=>{
          const cells=[]; let hw=0, aw=0;
          for(let g=1; g<=7; g++){
            const game = focusSeries.games?.[g-1] || {};
            const hs = game.homeScore, as = game.awayScore;
            const hasScore = typeof hs==="number" && typeof as==="number";
            if (hasScore) { if (hs>as) hw++; else if (as>hs) aw++; }
            const runningLabel = hasScore
              ? (hw===aw ? `${hw}-${aw}` : hw>aw ? `${focusSeries.homeAbbr} ${hw}-${aw}` : `${focusSeries.awayAbbr} ${aw}-${hw}`)
              : null;
            const bg = hasScore ? "rgba(16,185,129,0.12)" : "var(--color-background-secondary)";
            const borderCol = hasScore ? "rgba(16,185,129,0.3)" : "var(--color-border-tertiary)";
            cells.push(
              <div key={g} style={{padding:"5px 4px",textAlign:"center",background:bg,borderRadius:3,fontSize:9,border:`0.5px solid ${borderCol}`,minHeight:54}}>
                <div style={{fontSize:8,color:"var(--color-text-tertiary)",marginBottom:2}}>G{g}</div>
                {hasScore
                  ? <>
                      <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-primary)",fontFamily:"var(--font-mono)"}}>
                        {hs}-{as}{game.ot?<span style={{color:"#f59e0b",marginLeft:2}}>OT</span>:""}
                      </div>
                      <div style={{fontSize:8,color:"var(--color-text-secondary)",marginTop:1}}>{runningLabel}</div>
                    </>
                  : <div style={{fontSize:9,color:"var(--color-text-tertiary)",marginTop:6}}>—</div>
                }
              </div>
            );
          }
          return cells;
        })()}
      </div>
    </div>}

    {/* Imports log — v83: shows ALL imports across all series, not just current focus */}
    {imports.length>0&&<div style={{marginTop:12}}>
      <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:4}}>
        All Uploaded Games ({imports.length})
      </div>
      <div style={{maxHeight:240,overflowY:"auto",border:"0.5px solid var(--color-border-tertiary)",borderRadius:"var(--border-radius-md)"}}>
        {imports.map(e=>(
          <div key={e.id} style={{display:"flex",gap:8,fontSize:10,padding:"4px 8px",borderBottom:"0.5px solid var(--color-border-tertiary)",alignItems:"center"}}>
            <span style={{color:"var(--color-text-tertiary)",flexShrink:0,fontSize:9}}>{e.ts}</span>
            <span style={{color:"#10b981",flexShrink:0,fontFamily:"var(--font-mono)"}}>R{e.round||1}G{e.game}</span>
            <span style={{flexShrink:0,fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>
              {e.awayAbbr} {e.awayScore}-{e.homeScore} {e.homeAbbr}{e.ot?" OT":""}
            </span>
            <span style={{color:"var(--color-text-secondary)",flexShrink:0}}>{e.matched}p</span>
            {e.unmatched.length>0&&<span style={{color:"#f59e0b",fontSize:9}}>⚠{e.unmatched.length}</span>}
            {e._source&&<span style={{fontSize:8,color:"var(--color-text-tertiary)",letterSpacing:0.3}}>{e._source}</span>}
            <button onClick={()=>{
              if(window.confirm(`Undo ${e.awayAbbr} @ ${e.homeAbbr} G${e.game}? This will remove all stats from this game.`)) handleUndo(e.id);
            }}
              style={{marginLeft:"auto",padding:"2px 8px",fontSize:9,borderRadius:3,border:"0.5px solid rgba(239,68,68,0.3)",background:"rgba(239,68,68,0.08)",color:"#ef4444",cursor:"pointer"}}>
              ✕ Delete
            </button>
          </div>
        ))}
      </div>
    </div>}
  </div>;
}


// ═══════════════════════════════════════════════════════════════════════════════
// PLAYOFF TOTALS VERIFICATION TABLE (v13)
// Shows every player with pGP>0 or any recorded playoff stat. Lets user
// confirm Game Stat Import actually landed. Primary debug tool for import issues.
// ═══════════════════════════════════════════════════════════════════════════════
function PlayoffTotalsTable({players,dark}) {
  const [filterTeam,setFilterTeam]=useState("ALL");
  const [sortBy,setSortBy]=useState("pts");

  const rows=useMemo(()=>{
    if(!players) return [];
    return players
      .filter(p=>(p.pGP||0)>0 || (p.pG||0)>0 || (p.pA||0)>0 || (p.pSOG||0)>0)
      .filter(p=>filterTeam==="ALL"||p.team===filterTeam)
      .map(p=>({...p,pPts:(p.pG||0)+(p.pA||0)}))
      .sort((a,b)=>{
        if(sortBy==="team") return a.team.localeCompare(b.team)||b.pPts-a.pPts;
        if(sortBy==="pts") return b.pPts-a.pPts;
        if(sortBy==="g") return (b.pG||0)-(a.pG||0);
        if(sortBy==="sog") return (b.pSOG||0)-(a.pSOG||0);
        return 0;
      });
  },[players,filterTeam,sortBy]);

  const teams=useMemo(()=>{
    if(!players) return [];
    return [...new Set(players.filter(p=>(p.pGP||0)>0).map(p=>p.team))].sort();
  },[players]);

  if(!players) return <div style={{fontSize:11,color:"var(--color-text-tertiary)"}}>Load skaters.csv first.</div>;
  if(rows.length===0) return <div style={{fontSize:11,color:"var(--color-text-tertiary)"}}>No playoff game stats imported yet. After you paste a Hockey Reference box score above, matched players will appear here.</div>;

  return <div>
    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:8,fontSize:10,flexWrap:"wrap"}}>
      <span style={{color:"var(--color-text-secondary)"}}>{rows.length} players with playoff stats</span>
      <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)}
        style={{fontSize:10,padding:"2px 5px",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}>
        <option value="ALL">All teams</option>
        {teams.map(t=><option key={t} value={t}>{t}</option>)}
      </select>
      <select value={sortBy} onChange={e=>setSortBy(e.target.value)}
        style={{fontSize:10,padding:"2px 5px",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}>
        <option value="pts">Sort: Points</option>
        <option value="g">Sort: Goals</option>
        <option value="sog">Sort: SOG</option>
        <option value="team">Sort: Team</option>
      </select>
    </div>
    <div style={{maxHeight:280,overflowY:"auto",overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
        <thead style={{position:"sticky",top:0,background:dark?"#131625":"#fff",zIndex:1}}>
          <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
            {["Player","Team","GP","G","A","Pts","SOG"].map(h=>(
              <th key={h} style={{padding:"3px 6px",color:"var(--color-text-tertiary)",fontWeight:500,textAlign:h==="Player"||h==="Team"?"left":"right",fontSize:9}}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>{rows.map((p,i)=>(
          <tr key={p.name+p.team} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
            <td style={{padding:"2px 6px"}}>{p.name}</td>
            <td style={{padding:"2px 6px"}}><span style={{fontSize:8,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
            <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)"}}>{p.pGP||0}</td>
            <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)",color:(p.pG||0)>0?"#4ade80":"var(--color-text-tertiary)",fontWeight:(p.pG||0)>0?500:400}}>{p.pG||0}</td>
            <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)",color:(p.pA||0)>0?"#4ade80":"var(--color-text-tertiary)"}}>{p.pA||0}</td>
            <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500}}>{p.pPts}</td>
            <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>{p.pSOG||0}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  </div>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// UPLOAD TAB
// ═══════════════════════════════════════════════════════════════════════════════
function UploadTab({players,setPlayers,goalies,setGoalies,linemates,setLinemates,exportState,importState,syncStatus,allSeries,setAllSeries,dark,onGameUploaded,currentRound}) {
  const [fileErr,setFileErr]=useState("");
  const setErr=setFileErr; // alias used in file handlers

  const playerFileRef = useRef(null);
  const goalieFileRef = useRef(null);
  const hrRosterFileRef = useRef(null);
  const linesFileRef = useRef(null);

  // v29: Merge HR season-skater roster into existing players.
  // Strategy: for each HR player, try to match on (normalized name + team). If matched,
  // update the player's NAME to the HR spelling (so diacritics line up with box score pastes)
  // and overlay any HR-source fields that the existing record is missing/zero. Preserve
  // MoneyPuck's xG fields (onIceF/onIceA) and any playoff data (pGames/pG/pA/...).
  // If no match, add as a new player.
  function mergeHRRoster(hrPlayers) {
    if (!hrPlayers || !hrPlayers.length) return;
    const norm = s => (s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/[^a-z0-9]/g,"");
    const existing = players || [];
    // Index existing by (normName + team) for fast lookup
    const byKey = new Map(existing.map(p => [norm(p.name)+"|"+p.team, p]));
    const updated = [];
    const seenKeys = new Set();
    let nameFixed = 0, added = 0, teamMoved = 0;

    // First pass: walk HR players, attempt to match
    for (const hp of hrPlayers) {
      const k = norm(hp.name) + "|" + hp.team;
      // Direct match (same team)
      let match = byKey.get(k);
      // Cross-team match (player traded; HR has new team, existing has old team)
      if (!match) {
        for (const ep of existing) {
          if (norm(ep.name) === norm(hp.name)) { match = ep; break; }
        }
      }
      if (match) {
        seenKeys.add(norm(match.name)+"|"+match.team);
        const teamChanged = match.team !== hp.team;
        if (teamChanged) teamMoved++;
        const nameChanged = match.name !== hp.name;
        if (nameChanged) nameFixed++;
        updated.push({
          ...match,
          name: hp.name,    // adopt HR spelling (diacritics)
          team: hp.team,    // adopt HR team (post-trade)
          // Overlay HR rates ONLY where existing record has zero/missing — preserve MoneyPuck advanced data otherwise.
          gp: match.gp || hp.gp,
          g_pg:    match.g_pg    || hp.g_pg,
          a_pg:    match.a_pg    || hp.a_pg,
          sog_pg:  match.sog_pg  || hp.sog_pg,
          hit_pg:  match.hit_pg  || hp.hit_pg,
          blk_pg:  match.blk_pg  || hp.blk_pg,
          take_pg: match.take_pg || hp.take_pg,
          pim_pg:  match.pim_pg  || hp.pim_pg,
          give_pg: match.give_pg || hp.give_pg,
        });
      } else {
        updated.push(hp);
        added++;
      }
    }
    // Second pass: keep existing players who weren't in HR (e.g., on non-playoff teams or scratched all year).
    // We add them only if their team is in the playoff set; HR is authoritative for current rosters.
    for (const ep of existing) {
      const k = norm(ep.name)+"|"+ep.team;
      if (seenKeys.has(k)) continue;
      // Find by name only — if HR has them on a different team, they're already in `updated`
      const hrHasName = hrPlayers.some(hp => norm(hp.name) === norm(ep.name));
      if (hrHasName) continue; // matched cross-team, already in updated
      updated.push(ep);
    }

    updated.sort((a,b)=> (a.team||"").localeCompare(b.team||"") || (b.pts||0) - (a.pts||0));
    setPlayers(updated);
    setFileErr(`✓ HR roster merged: ${hrPlayers.length} HR players · ${added} added · ${nameFixed} names corrected · ${teamMoved} teams updated`);
  }

  function handleHRRosterFile(e) {
    const file = e.target.files[0]; if (!file) return;
    e.target.value = "";
    const reader = new FileReader();
    reader.onload = ev => {
      try {
        const r = parseHRSkaters(ev.target.result);
        if (r.error) { setFileErr("HR parse error: " + r.error); return; }
        mergeHRRoster(r.players);
      } catch (e) { setFileErr("HR parse error: " + e.message); }
    };
    reader.readAsText(file);
  }

  // Paste-based HR roster import (no file upload — paste CSV directly into a textarea)
  const [hrPasteText, setHrPasteText] = useState("");
  function handleHRRosterPaste() {
    if (!hrPasteText.trim()) return;
    try {
      const r = parseHRSkaters(hrPasteText);
      if (r.error) { setFileErr("HR parse error: " + r.error); return; }
      mergeHRRoster(r.players);
      setHrPasteText("");
    } catch (e) { setFileErr("HR parse error: " + e.message); }
  }

  function handlePlayerFile(e){
    const file=e.target.files[0];if(!file)return;
    e.target.value=""; // reset so same file can be re-selected
    const reader=new FileReader();
    reader.onload=ev=>{
      try{
        const text=ev.target.result;
        if(file.name.endsWith(".json")){const d=JSON.parse(text);setPlayers(Array.isArray(d)?d:d.players);return;}
        const lines=text.split("\n"),headers=lines[0].split(",");
        const idx=h=>headers.indexOf(h);
        const pt=new Set(["ANA","BOS","BUF","CAR","COL","DAL","EDM","LAK","MIN","MTL","OTT","PHI","PIT","TBL","UTA","VGK"]);
        const parsed=[];
        for(let i=1;i<lines.length;i++){
          const c=lines[i].split(",");
          if(!c[idx("situation")]||c[idx("situation")].trim()!=="all")continue;
          let team=c[idx("team")]?.trim();if(!pt.has(team))continue;if(team==="VGK")team="VEG";
          const gp=parseFloat(c[idx("games_played")])||1;
          const g=parseFloat(c[idx("I_F_goals")])||0;
          const a=(parseFloat(c[idx("I_F_primaryAssists")])||0)+(parseFloat(c[idx("I_F_secondaryAssists")])||0);
          const sog=parseFloat(c[idx("I_F_shotsOnGoal")])||0;
          const hit=parseFloat(c[idx("I_F_hits")])||0;
          const blk=parseFloat(c[idx("shotsBlockedByPlayer")])||0;
          const tk=parseFloat(c[idx("I_F_takeaways")])||0;
          const pim=parseFloat(c[idx("I_F_penalityMinutes")])||0;
          const tsa=parseFloat(c[idx("I_F_shotAttempts")])||0;
          const give=parseFloat(c[idx("I_F_giveaways")])||0;
          // v24: xG for team strength. onIce_F/A_xGoals for TOI-weighted team strength. icetime in seconds.
          const onIceF=parseFloat(c[idx("OnIce_F_xGoals")])||0;
          const onIceA=parseFloat(c[idx("OnIce_A_xGoals")])||0;
          const toi=parseFloat(c[idx("icetime")])||0;
          const posVal=c[idx("position")]?.trim()||"F";
          const name=c[idx("name")]?.trim();if(!name)continue;
          const defRole=posVal==="D"?"D2":posVal==="G"?"BACKUP":"MID6";
          parsed.push({name,team,pos:posVal,gp:Math.round(gp),g,a,pts:g+a,sog,hit,blk,tk,pim,tsa,give,
            onIceF,onIceA,toi,
            // v56: toi_pg in MINUTES per game (icetime is in seconds)
            toi_pg: gp > 0 ? +((toi / gp) / 60).toFixed(2) : 0,
            g_pg:+(g/gp).toFixed(4),a_pg:+(a/gp).toFixed(4),pts_pg:+((g+a)/gp).toFixed(4),
            sog_pg:+(sog/gp).toFixed(4),hit_pg:+(hit/gp).toFixed(4),blk_pg:+(blk/gp).toFixed(4),
            take_pg:+(tk/gp).toFixed(4),pim_pg:+(pim/gp).toFixed(4),
            tsa_pg:+(tsa/gp).toFixed(4),give_pg:+(give/gp).toFixed(4),
            lineRole:defRole,pGP:0,pG:0,pA:0,pSOG:0,pHIT:0,pBLK:0,pTK:0,pPIM:0,pTSA:0,pGIVE:0});
        }
        parsed.sort((a,b)=>a.team.localeCompare(b.team)||b.pts-a.pts);
        // v43: preserve user-set lineRole + accumulated playoff stats across skaters.csv reloads.
        // Without this, re-loading wipes every role tag the user has manually curated.
        // Match by normalized name+team; if found, merge the new regular-season data on top of
        // the existing record while keeping lineRole and pG/pA/pSOG/etc.
        const existing = players || [];
        if (existing.length) {
          const normName = (s) => (s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase().replace(/[^a-z0-9]/g,"");
          const byKey = new Map(existing.map(p => [normName(p.name)+"|"+(p.team||""), p]));
          const byNameOnly = new Map(existing.map(p => [normName(p.name), p]));
          const PRESERVE_FIELDS = ["lineRole","pGP","pG","pA","pPts","pSOG","pHIT","pBLK","pTK","pGV","pPIM","pTOI","pTSA","pGIVE"];
          const merged = parsed.map(np => {
            const key = normName(np.name) + "|" + np.team;
            let prior = byKey.get(key);
            if (!prior) prior = byNameOnly.get(normName(np.name)); // cross-team (traded player)
            if (!prior) return np;
            const out = {...np};
            for (const f of PRESERVE_FIELDS) {
              if (prior[f] != null) out[f] = prior[f];
            }
            return out;
          });
          setPlayers(merged);
        } else {
          setPlayers(parsed);
        }
      }catch(e){setErr("Skaters parse error: "+e.message);}
    };
    reader.readAsText(file);
  }

  function handleGoalieFile(e){
    const file=e.target.files[0];if(!file)return;
    e.target.value="";
    const reader=new FileReader();
    reader.onload=ev=>{
      try{
        const text=ev.target.result;
        if(file.name.endsWith(".json")){const d=JSON.parse(text);setGoalies(Array.isArray(d)?d:d.goalies);return;}
        const lines=text.split("\n"),headers=lines[0].split(",");
        const idx=h=>headers.indexOf(h);
        const pt=new Set(["ANA","BOS","BUF","CAR","COL","DAL","EDM","LAK","MIN","MTL","OTT","PHI","PIT","TBL","UTA","VGK"]);
        const rawGoalies=[];
        for(let i=1;i<lines.length;i++){
          const c=lines[i].split(",");
          if(!c[idx("situation")]||c[idx("situation")].trim()!=="all")continue;
          let team=c[idx("team")]?.trim();if(!pt.has(team))continue;if(team==="VGK")team="VEG";
          const gp=parseFloat(c[idx("games_played")])||1;
          const ongoal=parseFloat(c[idx("ongoal")])||0;
          const goals=parseFloat(c[idx("goals")])||0;
          const xGoals=parseFloat(c[idx("xGoals")])||goals; // v23: fall back to goals if xGoals missing
          const icetime=parseFloat(c[idx("icetime")])||0; // seconds
          const saves=ongoal-goals;
          const name=c[idx("name")]?.trim();if(!name)continue;
          // v75: goalie quality based on Goals Saved Above Expected per 60 (GSAx/60) with Bayesian shrinkage.
          // Old formula (xGoals/goals clamped 0.75-1.25) produced ~95% of goalies at 0.95-1.05 → near-zero
          // model effect on opposing skater scoring. New formula gives a meaningful 0.85-1.15 spread.
          //
          // Convention preserved: quality > 1.0 = ELITE (saves more than expected).
          // Consumer code applies multiplier as 1/quality on opposing skater rate → elite goalie shrinks scoring.
          //
          // Math:
          //   GSAx/60 = (xGoals - goals) / icetime_hours          // positive = elite
          //   raw_quality = 1 + 0.30 × GSAx/60                    // +0.5 GSAx/60 → 1.15 quality (elite)
          //   raw_quality clamped to [0.80, 1.20]
          //   shrinkage_weight = icetime_min / (icetime_min + 600)// half-prior at 10 game-equivalents
          //   quality = 1 + shrinkage_weight × (raw_quality - 1)  // shrink toward 1.0 on small samples
          let quality = 1.0;
          if (icetime > 0) {
            const gsax60 = (xGoals - goals) / (icetime / 3600);
            const rawQuality = Math.max(0.80, Math.min(1.20, 1 + 0.30 * gsax60));
            const icemin = icetime / 60;
            const w = icemin / (icemin + 600);
            quality = +(1 + w * (rawQuality - 1)).toFixed(3);
          }
          rawGoalies.push({name,team,gp:Math.round(gp),saves,saves_pg:+(saves/gp).toFixed(4),
                          xGoals:+xGoals.toFixed(2),goals:+goals.toFixed(1),quality});
        }
        const teamGP={};
        rawGoalies.forEach(g=>{teamGP[g.team]=(teamGP[g.team]||0)+g.gp;});
        const parsed=rawGoalies.map(g=>({...g,starter_share:+(g.gp/Math.max(1,teamGP[g.team])).toFixed(4),pGP:0,pSaves:0}));
        parsed.sort((a,b)=>a.team.localeCompare(b.team)||b.starter_share-a.starter_share);
        setGoalies(parsed);
      }catch(e){setErr("Goalies parse error: "+e.message);}
    };
    reader.readAsText(file);
  }

  // v57: parse MoneyPuck lines.csv → linemates map for assist correlation
  function handleLinesFile(e){
    const file=e.target.files[0];if(!file)return;
    e.target.value="";
    const reader=new FileReader();
    reader.onload=ev=>{
      try{
        const text = ev.target.result;
        const map = parseLinesCsv(text);
        const nLines = Object.keys(map).length;
        if (nLines === 0) { setErr("Lines parse: 0 players extracted. Check CSV has 'name', 'team', 'icetime', 'position=line', 'situation=5on5' columns."); return; }
        setLinemates(map);
        setErr(null);
      }catch(e){setErr("Lines parse error: "+e.message);}
    };
    reader.readAsText(file);
  }

  return (
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:20,alignItems:"start"}}>
      {/* Hidden real file inputs — triggered by visible buttons */}
      <input ref={playerFileRef} type="file" accept=".csv,.json" onChange={handlePlayerFile} style={{display:"none"}}/>
      <input ref={goalieFileRef} type="file" accept=".csv,.json" onChange={handleGoalieFile} style={{display:"none"}}/>
      <input ref={hrRosterFileRef} type="file" accept=".csv" onChange={handleHRRosterFile} style={{display:"none"}}/>
      <input ref={linesFileRef} type="file" accept=".csv" onChange={handleLinesFile} style={{display:"none"}}/>
      <div>
        <Card style={{marginBottom:14}}>
          <SH title="Game Stat Import" sub="Paste a Hockey Reference box score — stats are added to each player's running playoff totals"/>
          {/* Series + Game selector */}
          <GameStatImporter players={players} setPlayers={setPlayers} goalies={goalies} setGoalies={setGoalies} allSeries={allSeries} setAllSeries={setAllSeries} onGameUploaded={onGameUploaded}/>
        </Card>
        <Card style={{marginBottom:14}}>
          <SH title="Live Playoff Totals" sub="Post-import verification — any player with pGP > 0 appears here"/>
          <PlayoffTotalsTable players={players} dark={dark}/>
        </Card>
      </div>

      <div>
        <Card style={{marginBottom:14}}>
          <SH title="Skater Base Data"/>
          <div style={{marginBottom:8,fontSize:12,color:"var(--color-text-secondary)"}}>{players?`${players.length} skaters loaded`:"No skater data"}</div>
          <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"center"}}>
            <button onClick={()=>playerFileRef.current?.click()} style={{padding:"6px 16px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"#3b82f6",color:"white",border:"none",cursor:"pointer"}}>{players?"Re-load skaters.csv":"Load skaters.csv"}</button>
            <button onClick={()=>hrRosterFileRef.current?.click()} disabled={!players}
              title={players?"Merge a Hockey Reference season-skater CSV (fixes diacritics, adds late-season callups, updates traded players)":"Load skaters.csv first"}
              style={{padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",
                background:players?"#0d9488":"var(--color-background-secondary)",
                color:players?"white":"var(--color-text-tertiary)",border:"none",cursor:players?"pointer":"default"}}>
              + Merge HR Roster
            </button>
            <button onClick={()=>{
              if (!players) return;
              const before = players.length;
              const merged = dedupePlayers(players);
              const removed = before - merged.length;
              setPlayers(merged);
              alert(removed > 0 ? `Merged ${removed} duplicate player record${removed===1?"":"s"} (Stutzle/Stützle, Josh/Joshua Norris, etc.). Stats summed; longer name spelling kept.` : "No duplicates found.");
            }} disabled={!players}
              title="Find players with same normalized name+team (handles diacritics + Josh/Joshua-style nickname variants) and merge their stats."
              style={{padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",
                background:players?"#a16207":"var(--color-background-secondary)",
                color:players?"white":"var(--color-text-tertiary)",border:"none",cursor:players?"pointer":"default"}}>
              ↻ Dedupe Names
            </button>
          </div>
          {players&&<div style={{marginTop:10,display:"flex",gap:4,flexWrap:"wrap"}}>
            {["TOP6","MID6","BOT6","ON_ROSTER","ACTIVE","D1","D2","D3","STARTER","BACKUP","INACTIVE","IR","SCRATCHED"].map(r=>{
              const cnt=players.filter(p=>p.lineRole===r).length;
              if(!cnt) return null;
              const c=roleColor(r);
              return <div key={r} style={{fontSize:9,padding:"2px 7px",borderRadius:3,background:`${c}20`,color:c,fontWeight:500}}>{r}: {cnt}</div>;
            })}
          </div>}
          {/* Or paste HR CSV directly (avoids needing to save a file) */}
          {players && <details style={{marginTop:10}}>
            <summary style={{fontSize:10,color:"var(--color-text-tertiary)",cursor:"pointer",userSelect:"none"}}>
              Or paste HR roster CSV directly (no file save needed)
            </summary>
            <div style={{marginTop:6}}>
              <div style={{fontSize:9,color:"var(--color-text-tertiary)",marginBottom:4,lineHeight:1.5}}>
                From hockey-reference.com/leagues/NHL_2026_skaters.html → "Get table as CSV (for Excel)" → paste full text here.
                Multi-team rows (2TM/3TM) auto-resolve to most recent team.
              </div>
              <textarea value={hrPasteText} onChange={e=>setHrPasteText(e.target.value)}
                placeholder="Paste HR season-skater CSV (Rk,Player,Age,Team,...) here…"
                style={{width:"100%",height:80,fontSize:9,fontFamily:"var(--font-mono)",
                  background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
                  borderRadius:"var(--border-radius-md)",padding:8,color:"var(--color-text-primary)",
                  resize:"vertical",boxSizing:"border-box",marginBottom:6}}/>
              <button onClick={handleHRRosterPaste} disabled={!hrPasteText.trim()}
                style={{padding:"4px 12px",fontSize:11,borderRadius:"var(--border-radius-md)",border:"none",
                  cursor:hrPasteText.trim()?"pointer":"default",
                  background:hrPasteText.trim()?"#0d9488":"var(--color-background-secondary)",
                  color:hrPasteText.trim()?"white":"var(--color-text-tertiary)"}}>
                Merge from Paste
              </button>
            </div>
          </details>}
          {fileErr&&<div style={{marginTop:8,fontSize:11,color:fileErr.startsWith("✓")?"#10b981":"#ef4444"}}>{fileErr}</div>}
        </Card>

        <Card style={{marginBottom:14}}>
          <SH title="Goalie Data" sub="Required for Goalie Saves props in Series Pricer"/>
          <div style={{marginBottom:8,fontSize:12,color:"var(--color-text-secondary)"}}>{goalies?`${goalies.length} goalies loaded`:"No goalie data"}</div>
          <button onClick={()=>goalieFileRef.current?.click()} style={{padding:"6px 16px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"#7c3aed",color:"white",border:"none",cursor:"pointer",marginBottom:8}}>{goalies?"Re-load goalies.csv":"Load goalies.csv"}</button>
          <div style={{fontSize:10,color:"var(--color-text-tertiary)",lineHeight:1.7}}>
            Upload the MoneyPuck <code style={{background:"var(--color-background-secondary)",padding:"0 3px",borderRadius:2}}>goalies.csv</code> (situation=all).<br/>
            Saves = ongoal − goals. Starter share = GP / team total GP.
          </div>
          {goalies&&<div style={{marginTop:10,overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
              <TH cols={["Goalie","Team","GP","Sv/G","Share"]}/>
              <tbody>{goalies.filter(g=>g.starter_share>=0.15).slice(0,16).map((g,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                  <td style={{padding:"2px 6px"}}>{g.name}</td>
                  <td style={{padding:"2px 6px",textAlign:"right"}}><span style={{fontSize:8,padding:"1px 4px",borderRadius:2,background:"rgba(124,58,237,0.15)",color:"#a78bfa"}}>{g.team}</span></td>
                  <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)"}}>{g.gp}</td>
                  <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)"}}>{g.saves_pg.toFixed(1)}</td>
                  <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>{(g.starter_share*100).toFixed(0)}%</td>
                </tr>
              ))}</tbody>
            </table>
            {goalies.filter(g=>g.starter_share>=0.15).length>16&&<div style={{fontSize:9,color:"var(--color-text-tertiary)",marginTop:4}}>Showing starters only (≥15% share)</div>}
          </div>}
        </Card>

        <Card style={{marginBottom:14}}>
          <SH title="Linemate Data (optional)" sub="Fixes assist correlation — goalscorer's linemates get weighted higher for assists"/>
          <div style={{marginBottom:8,fontSize:12,color:"var(--color-text-secondary)"}}>
            {linemates && Object.keys(linemates).length>0 ? `${Object.keys(linemates).length} players mapped with linemates` : "No line data loaded"}
          </div>
          <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:8}}>
            <button onClick={()=>linesFileRef.current?.click()} style={{padding:"6px 16px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"#0d9488",color:"white",border:"none",cursor:"pointer"}}>
              {linemates && Object.keys(linemates).length>0 ? "Re-load lines.csv" : "Load lines.csv"}
            </button>
            {linemates && Object.keys(linemates).length>0 && <button onClick={()=>{
              if(confirm("Clear linemates map? (reverts to independent-assist model)")) setLinemates({});
            }} style={{padding:"5px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",color:"var(--color-text-secondary)",border:"0.5px solid var(--color-border-secondary)",cursor:"pointer"}}>Clear</button>}
          </div>
          <div style={{fontSize:10,color:"var(--color-text-tertiary)",lineHeight:1.7}}>
            Upload MoneyPuck <code style={{background:"var(--color-background-secondary)",padding:"0 3px",borderRadius:2}}>lines.csv</code> (any season). Filtered to forward lines at 5on5.<br/>
            For each goalscorer, top-3 linemates by shared TOI get 3× assist weight boost in the sim.
          </div>
        </Card>

        <Card style={{border:"0.5px solid rgba(239,68,68,0.25)"}}>
          <SH title="Reset Playoff Data" sub="Wipe all entered game results + player playoff stats. Keeps team abbreviations, win%, and expected total inputs."/>
          <button onClick={()=>{
            if (!confirm("Reset all playoff stats and game results?\n\n• All player pGames arrays → empty\n• All player pG/pA/pSOG/pHIT/pBLK/pTK/pPIM/pTSA/pGIVE → 0\n• All series game results (winners, scores) → cleared\n• Team abbreviations, win%, expTotal are PRESERVED\n\nThis cannot be undone.")) return;
            // Wipe player playoff stats
            if (players) {
              const wiped = players.map(p => ({
                ...p,
                pGames: [],
                pGP: 0, pG: 0, pA: 0, pSOG: 0, pHIT: 0, pBLK: 0, pTK: 0, pPIM: 0, pTSA: 0, pGIVE: 0,
              }));
              setPlayers(wiped);
            }
            // Clear all series game results (keep winPct/expTotal) — round-aware
            setAllSeries(prev => {
              const out = {};
              for (const r of ROUND_IDS) {
                out[r] = (prev[r]||[]).map(sr => ({
                  ...sr,
                  games: (sr.games||[]).map(g => ({
                    ...g,
                    result: null, homeScore: null, awayScore: null, wentOT: false,
                  })),
                }));
              }
              return out;
            });
          }} style={{padding:"6px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"#ef4444",color:"white",border:"none",cursor:"pointer",fontWeight:500}}>
            Wipe All Playoff Stats
          </button>
        </Card>

        <Card>
          <SH title="JSON Backup" sub="Export state to copy-paste, or paste a backup below to restore"/>
          <ExportImportPanel exportState={exportState} importState={importState}/>
        </Card>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROLES TAB
// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// PLAYER STATS TAB (v20)
// Per-game stats history, editable. Round tabs, team filter, search.
// Click a cell to open edit modal with all 8 stat inputs.
// ═══════════════════════════════════════════════════════════════════════════════
function PlayerStatsTab({players,setPlayers,dark}) {
  const [round,setRound]=useState(1);
  const [filterTeam,setFilterTeam]=useState("ALL");
  const [search,setSearch]=useState("");
  const [showZeros,setShowZeros]=useState(false);
  const [editing,setEditing]=useState(null); // {player, round, game}
  // v76: pivot table by selected stat. Default Points.
  const [sortStat,setSortStat]=useState("rPts");
  const SORT_TABS=[
    {id:"rPts",l:"Points"},{id:"rG",l:"Goals"},{id:"rA",l:"Assists"},
    {id:"rSOG",l:"SOG"},{id:"rHIT",l:"Hits"},{id:"rBLK",l:"Blocks"},
    {id:"rTK",l:"TK"},{id:"rGIVE",l:"GV"},{id:"rPIM",l:"PIM"},
  ];

  // Active roster: everyone except SCRATCHED/STARTER/BACKUP
  const roster = useMemo(()=>{
    if(!players) return [];
    return players
      .map(migratePlayer)
      .filter(p=>!isOutGlobally(p)&&p.lineRole!=="STARTER"&&p.lineRole!=="BACKUP")
      .filter(p=>filterTeam==="ALL"||p.team===filterTeam)
      .filter(p=>!search||p.name.toLowerCase().includes(search.toLowerCase()));
  },[players,filterTeam,search]);

  // Per-round view
  const rows = useMemo(()=>{
    return roster.map(p=>{
      const games={};
      let rGP=0,rG=0,rA=0,rSOG=0,rHIT=0,rBLK=0,rTK=0,rPIM=0,rGIVE=0;
      for (const e of (p.pGames||[])) {
        if (e.round===round) {
          games[e.game]=e;
          rGP++;
          rG += e.g||0; rA += e.a||0; rSOG += e.sog||0;
          rHIT += e.hit||0; rBLK += e.blk||0; rTK += e.tk||0;
          rPIM += e.pim||0; rGIVE += e.give||0;
        }
      }
      return {p, games, rGP, rG, rA, rPts:rG+rA, rSOG, rHIT, rBLK, rTK, rPIM, rGIVE};
    })
    .filter(r=>showZeros||r.rGP>0)
    .sort((a,b)=>(b[sortStat]||0)-(a[sortStat]||0) || b.rPts-a.rPts || a.p.name.localeCompare(b.p.name));
  },[roster,round,showZeros,sortStat]);

  const teams = useMemo(()=>{
    if(!players) return [];
    return [...new Set(players
      .filter(p=>!isOutGlobally(p)&&p.lineRole!=="STARTER"&&p.lineRole!=="BACKUP")
      .map(p=>p.team))].sort();
  },[players]);

  function saveEntry(playerName, playerTeam, round, game, statsPatch) {
    setPlayers(prev=>prev.map(p=>{
      if (p.name!==playerName || p.team!==playerTeam) return p;
      const base = migratePlayer(p);
      const existingGames = base.pGames || [];
      // Remove any existing entry for this round+game
      const others = existingGames.filter(e=>!(e.round===round && e.game===game));
      // Build new entry (empty stats default to 0)
      const newEntry = {round, game,
        g:+statsPatch.g||0, a:+statsPatch.a||0, sog:+statsPatch.sog||0,
        hit:+statsPatch.hit||0, blk:+statsPatch.blk||0, tk:+statsPatch.tk||0,
        pim:+statsPatch.pim||0, give:+statsPatch.give||0,
        _source:"manual_edit",
      };
      // If all stats are 0 AND user didn't explicitly set _keep, remove entry entirely
      const hasAnyStat = Object.values(newEntry).some(v=>typeof v==="number"&&v>0);
      const finalGames = (hasAnyStat || statsPatch._keep) ? [...others, newEntry] : others;
      return withRollups({...base, pGames:finalGames});
    }));
  }

  function deleteEntry(playerName, playerTeam, round, game) {
    setPlayers(prev=>prev.map(p=>{
      if (p.name!==playerName || p.team!==playerTeam) return p;
      if (!p.pGames) return p;
      const filtered = p.pGames.filter(e=>!(e.round===round && e.game===game));
      if (filtered.length===p.pGames.length) return p;
      return withRollups({...p, pGames:filtered});
    }));
  }

  if (!players) {
    return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Load skaters.csv on the Upload Stats tab first.</div></Card>;
  }

  return <div>
    <Card style={{marginBottom:12}}>
      <div style={{display:"flex",gap:10,alignItems:"center",flexWrap:"wrap"}}>
        <SH title="Player Stats" sub={`Round ${round} · ${rows.length} players`}/>
        <Seg options={[{id:1,label:"R1"},{id:2,label:"R2"},{id:3,label:"R3"},{id:4,label:"R4"}]} value={round} onChange={setRound} accent="#3b82f6"/>
        <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)}
          style={{fontSize:11,padding:"4px 8px",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:4,color:"var(--color-text-primary)"}}>
          <option value="ALL">All Teams</option>
          {teams.map(t=><option key={t} value={t}>{t}</option>)}
        </select>
        <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search player…"
          style={{fontSize:11,padding:"4px 8px",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:4,color:"var(--color-text-primary)",width:160}}/>
        <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center",cursor:"pointer"}}>
          <input type="checkbox" checked={showZeros} onChange={e=>setShowZeros(e.target.checked)}/>
          Show zero-GP
        </label>
        <span style={{marginLeft:"auto",fontSize:10,color:"var(--color-text-tertiary)"}}>Click any G1-G7 cell to edit</span>
      </div>
      {/* v76: sort/filter by stat. Default Points. */}
      <div style={{display:"flex",gap:0,marginTop:10,borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)",width:"fit-content"}}>
        {SORT_TABS.map(t => <button key={t.id} onClick={()=>setSortStat(t.id)} style={{
          padding:"5px 14px",fontSize:11,border:"none",
          borderRight:"0.5px solid var(--color-border-tertiary)",cursor:"pointer",
          background:sortStat===t.id?"#3b82f6":"var(--color-background-secondary)",
          color:sortStat===t.id?"white":"var(--color-text-secondary)",fontWeight:sortStat===t.id?500:400}}>
          {t.l}
        </button>)}
      </div>
    </Card>

    <Card>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <thead>
            <tr style={{borderBottom:"0.5px solid var(--color-border-secondary)"}}>
              <th style={{padding:"5px 8px",textAlign:"left",fontSize:10,fontWeight:500,color:"var(--color-text-secondary)"}}>Player</th>
              <th style={{padding:"5px 4px",textAlign:"center",fontSize:10,fontWeight:500,color:"var(--color-text-secondary)"}}>Team</th>
              <th style={{padding:"5px 4px",textAlign:"center",fontSize:10,fontWeight:500,color:"var(--color-text-secondary)"}}>Role</th>
              {[1,2,3,4,5,6,7].map(g=>(
                <th key={g} style={{padding:"5px 4px",textAlign:"center",fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",minWidth:64}}>G{g}</th>
              ))}
              <th style={{padding:"5px 8px",textAlign:"center",fontSize:10,fontWeight:500,color:"var(--color-text-primary)",background:"rgba(59,130,246,0.08)",borderLeft:"0.5px solid var(--color-border-tertiary)"}}>R{round} Totals</th>
            </tr>
          </thead>
          <tbody>
            {rows.length===0 && (
              <tr><td colSpan={11} style={{padding:"20px",textAlign:"center",fontSize:11,color:"var(--color-text-tertiary)"}}>
                {players.length===0 ? "No players loaded." : showZeros ? "No players match the filter." : "No players with R"+round+" game data yet. Import via Upload Stats tab, or toggle 'Show zero-GP' to edit players without entries."}
              </td></tr>
            )}
            {rows.map((r,i)=>{
              const p=r.p;
              // v83: map current sort tab to a per-game stat key. Cells now show only that stat.
              // "Played but no <stat>" gets a muted "0" (still shows they played); "didn't play" stays "+".
              const STAT_TAB_TO_GAME_KEY = {rG:"g", rA:"a", rPts:"pts", rSOG:"sog", rHIT:"hit", rBLK:"blk", rTK:"tk", rGIVE:"give", rPIM:"pim"};
              const STAT_TAB_TO_LABEL = {rG:"G", rA:"A", rPts:"P", rSOG:"SOG", rHIT:"H", rBLK:"B", rTK:"T", rGIVE:"GV", rPIM:"PIM"};
              const gKey = STAT_TAB_TO_GAME_KEY[sortStat] || "g";
              const sLabel = STAT_TAB_TO_LABEL[sortStat] || "";
              return <tr key={p.name+p.team} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.015)":"rgba(0,0,0,0.01)")}}>
                <td style={{padding:"4px 8px",fontSize:11}}>{p.name}</td>
                <td style={{padding:"4px 4px",textAlign:"center"}}><span style={{fontSize:9,padding:"1px 5px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
                <td style={{padding:"4px 4px",textAlign:"center"}}><RoleBadge role={p.lineRole}/></td>
                {[1,2,3,4,5,6,7].map(g=>{
                  const e = r.games[g];
                  const hasEntry = !!e;
                  // v83: per-game stat value matching the active sort tab.
                  const statVal = hasEntry ? (gKey === "pts" ? (e.g||0) + (e.a||0) : (e[gKey]||0)) : 0;
                  const hasThisStat = hasEntry && statVal > 0;
                  return <td key={g}
                    onClick={()=>setEditing({name:p.name,team:p.team,round,game:g,existing:e})}
                    style={{
                      padding:"4px 4px",textAlign:"center",cursor:"pointer",
                      fontFamily:"var(--font-mono)",fontSize:10,
                      // Three states: has the active stat (green), played but no stat (muted gray bg, no color), didn't play (dashed border)
                      background: hasThisStat ? "rgba(16,185,129,0.08)" : hasEntry ? "rgba(100,116,139,0.05)" : "transparent",
                      color: hasThisStat ? "#10b981" : hasEntry ? "var(--color-text-tertiary)" : "var(--color-text-tertiary)",
                      border: hasEntry ? `0.5px solid ${hasThisStat ? "rgba(16,185,129,0.2)" : "var(--color-border-tertiary)"}` : "0.5px dashed var(--color-border-tertiary)",
                      minWidth:56,
                      opacity: hasEntry ? 1 : 0.5,
                    }}
                    title={hasEntry?`G:${e.g} A:${e.a} SOG:${e.sog} HIT:${e.hit} BLK:${e.blk} TK:${e.tk} GIVE:${e.give}`:"Click to add stats"}>
                    {hasEntry ? (statVal === 0 ? "0" : `${statVal}${sLabel}`) : "+"}
                  </td>;
                })}
                <td style={{padding:"4px 8px",textAlign:"center",fontFamily:"var(--font-mono)",fontSize:10,background:"rgba(59,130,246,0.04)",borderLeft:"0.5px solid var(--color-border-tertiary)"}}>
                  {r.rGP>0 ? <div>
                    <div style={{fontWeight:500}}>{r.rG}G {r.rA}A · {r.rPts}P</div>
                    <div style={{fontSize:9,color:"var(--color-text-tertiary)"}}>{r.rSOG}SOG · {r.rHIT}H · {r.rBLK}B</div>
                  </div> : <span style={{color:"var(--color-text-tertiary)"}}>—</span>}
                </td>
              </tr>;
            })}
          </tbody>
        </table>
      </div>
    </Card>

    {editing && <PlayerStatEditModal
      editing={editing}
      onSave={(patch)=>{saveEntry(editing.name,editing.team,editing.round,editing.game,patch);setEditing(null);}}
      onDelete={()=>{deleteEntry(editing.name,editing.team,editing.round,editing.game);setEditing(null);}}
      onClose={()=>setEditing(null)}
      dark={dark}/>}
  </div>;
}

// Edit modal for a single game entry
function PlayerStatEditModal({editing,onSave,onDelete,onClose,dark}) {
  const init = editing.existing || {g:0,a:0,sog:0,hit:0,blk:0,tk:0,pim:0,give:0};
  const [v,setV]=useState({
    g:init.g||0, a:init.a||0, sog:init.sog||0,
    hit:init.hit||0, blk:init.blk||0, tk:init.tk||0,
    pim:init.pim||0, give:init.give||0,
  });
  const update=(k,x)=>setV(prev=>({...prev,[k]:x}));

  const overlay={position:"fixed",inset:0,zIndex:9999,background:"rgba(0,0,0,0.7)",display:"flex",alignItems:"center",justifyContent:"center",padding:"40px 16px"};
  const panel={background:dark?"#131625":"#fff",borderRadius:"var(--border-radius-lg)",padding:"18px 20px",maxWidth:440,width:"100%",border:"0.5px solid var(--color-border-secondary)"};
  const row={display:"grid",gridTemplateColumns:"90px 1fr",gap:8,alignItems:"center",marginBottom:8};
  const label={fontSize:11,color:"var(--color-text-secondary)"};
  const inp={padding:"5px 8px",fontSize:12,fontFamily:"var(--font-mono)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:4,color:"var(--color-text-primary)",width:"100%",boxSizing:"border-box"};

  return <div style={overlay} onClick={onClose}>
    <div style={panel} onClick={e=>e.stopPropagation()}>
      <div style={{display:"flex",alignItems:"baseline",justifyContent:"space-between",marginBottom:14}}>
        <div>
          <div style={{fontSize:14,fontWeight:500}}>{editing.name}</div>
          <div style={{fontSize:11,color:"var(--color-text-secondary)"}}>{editing.team} · R{editing.round} G{editing.game}</div>
        </div>
        <button onClick={onClose} style={{border:"none",background:"transparent",color:"var(--color-text-tertiary)",fontSize:16,cursor:"pointer"}}>×</button>
      </div>
      {[
        ["Goals","g"],["Assists","a"],["SOG","sog"],["Hits","hit"],
        ["Blocks","blk"],["Takeaways","tk"],["Giveaways","give"],
      ].map(([lab,k])=>(
        <div key={k} style={row}>
          <span style={label}>{lab}</span>
          <input type="number" min={0} max={99} step={1} value={v[k]}
            onChange={e=>update(k,parseInt(e.target.value)||0)}
            style={inp}/>
        </div>
      ))}
      <div style={{display:"flex",gap:8,marginTop:14,paddingTop:12,borderTop:"0.5px solid var(--color-border-tertiary)"}}>
        <button onClick={()=>onSave(v)}
          style={{padding:"7px 18px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",background:"#10b981",color:"white",cursor:"pointer"}}>
          Save
        </button>
        {editing.existing && <button onClick={onDelete}
          style={{padding:"7px 14px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"0.5px solid rgba(239,68,68,0.3)",background:"rgba(239,68,68,0.08)",color:"#ef4444",cursor:"pointer"}}>
          Delete entry
        </button>}
        <button onClick={onClose}
          style={{marginLeft:"auto",padding:"7px 14px",fontSize:12,borderRadius:"var(--border-radius-md)",border:"0.5px solid var(--color-border-secondary)",background:"var(--color-background-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>
          Cancel
        </button>
      </div>
      {editing.existing && editing.existing._migrated && <div style={{marginTop:10,fontSize:10,color:"#f59e0b",padding:"6px 8px",background:"rgba(245,158,11,0.08)",borderRadius:4}}>
        ⚠ This entry was migrated from legacy totals. Edit to split into real per-game entries.
      </div>}
    </div>
  </div>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROLES TAB
// ═══════════════════════════════════════════════════════════════════════════════
function RolesTab({players,setPlayers,dark}) {
  const [filterTeam,setFilterTeam]=useState("ALL");
  const [search,setSearch]=useState("");
  const teams=players?[...new Set(players.map(p=>p.team))].sort():[];
  // v91: sort CUT players to bottom
  const displayed=players?players
    .filter(p=>(filterTeam==="ALL"||p.team===filterTeam)&&(!search||p.name.toLowerCase().includes(search.toLowerCase())))
    .sort((a,b)=>{
      const aCut = canonicalRole(a.lineRole)==="CUT" ? 1 : 0;
      const bCut = canonicalRole(b.lineRole)==="CUT" ? 1 : 0;
      if (aCut !== bCut) return aCut - bCut;
      return 0;
    }):[];

  function setRole(name,team,role){setPlayers(prev=>prev.map(p=>p.name===name&&p.team===team?{...p,lineRole:role}:p));}
  function bulkSet(role){if(!filterTeam||filterTeam==="ALL")return;setPlayers(prev=>prev.map(p=>p.team===filterTeam?{...p,lineRole:role}:p));}

  if(!players) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12,padding:8}}>Load player data first</div></Card>;

  // v91: full taxonomy. Legacy roles (ON_ROSTER/SCRATCHED/INACTIVE) are auto-migrated on load
  // by migratePlayer; we still list them in the dropdown so old data displays correctly until edited.
  const ALL_ROLES=["TOP6","MID6","BOT6","D1","D2","D3","ACTIVE","D2D","STARTER","BACKUP","IR","CUT"];

  return <div>
    {/* v91: explanation card at top */}
    <Card style={{marginBottom:12}}>
      <SH title="Role Tag Reference" sub="How each tag affects pricing"/>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))",gap:10,fontSize:11,marginTop:8}}>
        <div>
          <div style={{fontWeight:500,color:"#10b981",fontSize:10,letterSpacing:0.4,marginBottom:4}}>FORWARDS</div>
          <div style={{color:"var(--color-text-secondary)",lineHeight:1.7}}>
            <span style={{color:"#10b981",fontWeight:500}}>TOP6</span> — first/second-line F (×1.12 scoring)<br/>
            <span style={{color:"#64748b",fontWeight:500}}>MID6</span> — third-line F (baseline)<br/>
            <span style={{color:"#f59e0b",fontWeight:500}}>BOT6</span> — fourth-line F (×0.90 scoring, ×1.10 hits)
          </div>
        </div>
        <div>
          <div style={{fontWeight:500,color:"#3b82f6",fontSize:10,letterSpacing:0.4,marginBottom:4}}>DEFENSE</div>
          <div style={{color:"var(--color-text-secondary)",lineHeight:1.7}}>
            <span style={{color:"#3b82f6",fontWeight:500}}>D1</span> — top pair (×1.03 scoring)<br/>
            <span style={{color:"#60a5fa",fontWeight:500}}>D2</span> — second pair (baseline)<br/>
            <span style={{color:"#93c5fd",fontWeight:500}}>D3</span> — third pair (×0.85 scoring, ×1.08 blocks)
          </div>
        </div>
        <div>
          <div style={{fontWeight:500,color:"#a78bfa",fontSize:10,letterSpacing:0.4,marginBottom:4}}>GOALIES</div>
          <div style={{color:"var(--color-text-secondary)",lineHeight:1.7}}>
            <span style={{color:"#a78bfa",fontWeight:500}}>STARTER</span> — projected starter for series<br/>
            <span style={{color:"#7c3aed",fontWeight:500}}>BACKUP</span> — second goalie<br/>
            <span style={{color:"var(--color-text-tertiary)"}}>Per-game starter set in Series Pricer</span>
          </div>
        </div>
        <div>
          <div style={{fontWeight:500,color:"#0ea5e9",fontSize:10,letterSpacing:0.4,marginBottom:4}}>STATUS (FLEX)</div>
          <div style={{color:"var(--color-text-secondary)",lineHeight:1.7}}>
            <span style={{color:"#0ea5e9",fontWeight:500}}>ACTIVE</span> — healthy scratch (on roster, not playing)<br/>
            <span style={{color:"#fbbf24",fontWeight:500}}>D2D</span> — out next game only, plays rest of series
          </div>
        </div>
        <div>
          <div style={{fontWeight:500,color:"#dc2626",fontSize:10,letterSpacing:0.4,marginBottom:4}}>STATUS (OUT)</div>
          <div style={{color:"var(--color-text-secondary)",lineHeight:1.7}}>
            <span style={{color:"#dc2626",fontWeight:500}}>IR</span> — out for series (excluded from this series's props only)<br/>
            <span style={{color:"#7f1d1d",fontWeight:500}}>CUT</span> — not on active roster (excluded from ALL props, grayed out)
          </div>
        </div>
        <div>
          <div style={{fontWeight:500,color:"var(--color-text-tertiary)",fontSize:10,letterSpacing:0.4,marginBottom:4}}>BEHAVIOR</div>
          <div style={{color:"var(--color-text-secondary)",lineHeight:1.6,fontSize:10}}>
            D2D players project for (remaining_games − 1). After their next game, update the tag (D2D → BOT6 if back, or stays D2D if still out).<br/>
            IR is per-series; player remains in DB and unflags between rounds.
          </div>
        </div>
      </div>
    </Card>

    <Card style={{marginBottom:12}}>
      <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
        <input placeholder="Search…" value={search} onChange={e=>setSearch(e.target.value)}
          style={{padding:"5px 10px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",width:180}}/>
        <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)} style={SEL}>
          <option value="ALL" style={{background:dark?"#131625":"#fff",color:dark?"#e7e9ee":"#0f172a"}}>All Teams</option>
          {teams.map(t=><option key={t} value={t} style={{background:dark?"#131625":"#fff",color:dark?"#e7e9ee":"#0f172a"}}>{t} – {TEAM_NAMES[t]}</option>)}
        </select>
        {filterTeam!=="ALL"&&<>
          <span style={{fontSize:10,color:"var(--color-text-secondary)"}}>Bulk (all):</span>
          {["IR","CUT"].map(r=><button key={r} onClick={()=>bulkSet(r)} style={{padding:"3px 8px",fontSize:9,borderRadius:3,border:"none",cursor:"pointer",fontWeight:500,background:`${roleColor(r)}20`,color:roleColor(r)}}>→{r}</button>)}
        </>}
        <div style={{marginLeft:"auto",display:"flex",gap:4,flexWrap:"wrap"}}>
          {ALL_ROLES.map(r=>{const cnt=players.filter(p=>canonicalRole(p.lineRole)===r).length;if(!cnt)return null;const c=roleColor(r);return <div key={r} style={{fontSize:9,padding:"2px 7px",borderRadius:3,background:`${c}20`,color:c,fontWeight:500}}>{r}: {cnt}</div>;})}
        </div>
      </div>
    </Card>

    <Card>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <TH cols={["Player","Team","Pos","GP","G","A","PTS","SOG","HIT","BLK","Role","pGP","pG","pA","pSOG","pHIT","pBLK","pTK","pTSA","pGV"]}/>
          <tbody>{displayed.slice(0,300).map((p,i)=>{
            const roles=rolesForPos(p.pos);
            const cur=canonicalRole(p.lineRole);
            const curRole=p.lineRole||roles[0];
            const isCut = cur === "CUT";
            const isOut = cur === "IR" || cur === "CUT";
            return (
            <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",
              background:isCut?(dark?"rgba(127,29,29,0.10)":"rgba(127,29,29,0.05)"):isOut?(dark?"rgba(239,68,68,0.06)":"rgba(239,68,68,0.04)"):i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
              <td style={{padding:"3px 8px",opacity:isOut?0.45:1,textDecoration:isCut?"line-through":"none"}}>
                {p.name}
                {(p.pG||p.pA) ? <span style={{marginLeft:8,fontSize:9,padding:"1px 5px",borderRadius:3,background:"rgba(34,197,94,0.15)",color:"#4ade80",fontFamily:"var(--font-mono)"}} title="Current playoff G-A-Pts">
                  {p.pG||0}G-{p.pA||0}A
                </span> : null}
              </td>
              <td style={{padding:"3px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
              <td style={{padding:"3px 8px",textAlign:"right",color:"var(--color-text-secondary)"}}>{p.pos}</td>
              {["gp","g","a","pts","sog","hit","blk"].map(f=><td key={f} style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{Math.round(p[f]||0)}</td>)}
              <td style={{padding:"3px 6px",textAlign:"right"}}>
                <select value={curRole} onChange={e=>setRole(p.name,p.team,e.target.value)}
                  style={{fontSize:9,padding:"2px 4px",background:`${roleColor(curRole)}18`,border:`0.5px solid ${roleColor(curRole)}`,borderRadius:3,color:roleColor(curRole),fontWeight:500}}>
                  {roles.map(r=><option key={r} value={r} style={{background:dark?"#131625":"#fff",color:dark?"#e7e9ee":"#0f172a"}}>{r}</option>)}
                </select>
              </td>
              {["pGP","pG","pA","pSOG","pHIT","pBLK","pTK","pTSA","pGIVE"].map(f=>(
                <td key={f} style={{padding:"1px 3px"}}>
                  <input type="number" value={p[f]||0} min={0} step={1}
                    onChange={e=>setPlayers(prev=>prev.map(q=>q.name===p.name&&q.team===p.team?{...q,[f]:parseInt(e.target.value)||0}:q))}
                    style={{width:32,fontSize:9,textAlign:"center",padding:"2px 2px",fontFamily:"var(--font-mono)",
                      background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}/>
                </td>
              ))}
            </tr>
            );
          })}
          {displayed.length>300&&<tr><td colSpan={20} style={{padding:8,textAlign:"center",color:"var(--color-text-tertiary)",fontSize:10}}>Showing 300/{displayed.length} — filter by team</td></tr>}
          </tbody>
        </table>
      </div>
    </Card>
  </div>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SERIES PARLAY PRICER TAB (v69)
// ═══════════════════════════════════════════════════════════════════════════════
// Pulls each active series's home/away advance probability (with margin already applied
// — same number shown in Quick Summary), then enumerates all valid combinations of
// 2..N teams across all current-round series. Excludes combos containing both teams
// from the same series (mathematically impossible). Per-team price overrides are
// LOCAL to this tab — they do not affect any other market or sheet.
function ParlayTab({allSeries, currentRound, margins, dark}) {
  const [showDec, setShowDec] = useState(true);
  const [overrides, setOverrides] = useState({}); // key: "<rid>|<abbr>" -> decimal odds
  const [selectedSize, setSelectedSize] = useState(2);

  // Build per-team option list from the active round's series.
  const teamOptions = useMemo(() => {
    const opts = [];
    (allSeries || []).forEach((s, si) => {
      if (!s || !s.games) return;
      // Drop completed series — winner has already advanced, no parlay action.
      const homeWins = s.games.filter(g => g.result === "home").length;
      const awayWins = s.games.filter(g => g.result === "away").length;
      if (homeWins >= 4 || awayWins >= 4) return;
      const outcomes = computeOutcomes(s.games);
      const hwp = ["4-0","4-1","4-2","4-3"].reduce((acc,k)=>acc+(outcomes[k]||0),0);
      const awp = 1 - hwp;
      const [adjH, adjA] = applyMargin([hwp, awp], margins.winner);
      const sid = `s${si}`;
      if (adjH > 0.001) opts.push({
        sid, team: s.homeTeam || s.homeAbbr || `Home${si+1}`,
        abbr: s.homeAbbr || "", modelP: adjH, modelDec: toDec(adjH),
      });
      if (adjA > 0.001) opts.push({
        sid, team: s.awayTeam || s.awayAbbr || `Away${si+1}`,
        abbr: s.awayAbbr || "", modelP: adjA, modelDec: toDec(adjA),
      });
    });
    return opts;
  }, [allSeries, margins.winner]);

  // Effective decimal per team: override if present, else model.
  const effDec = (opt) => {
    const k = `${opt.sid}|${opt.abbr}`;
    return overrides[k] != null ? overrides[k] : opt.modelDec;
  };

  // Generate all combinations of `k` teams from teamOptions, excluding same-series pairs.
  const buildCombos = (k) => {
    const result = [];
    const n = teamOptions.length;
    const idxs = new Array(k).fill(0).map((_,i)=>i);
    function recurse(start, picked) {
      if (picked.length === k) {
        // Check no two share a series
        const sids = new Set();
        for (const p of picked) {
          if (sids.has(p.sid)) return;
          sids.add(p.sid);
        }
        result.push(picked.slice());
        return;
      }
      for (let i = start; i < n; i++) {
        // Early prune: skip if same-series as any already picked
        if (picked.some(p => p.sid === teamOptions[i].sid)) continue;
        picked.push(teamOptions[i]);
        recurse(i + 1, picked);
        picked.pop();
      }
    }
    if (k <= n) recurse(0, []);
    return result;
  };

  const N = teamOptions.length / 2; // number of active series (rough)
  const sizes = [];
  // Max parlay size = number of active series (one team per series)
  // Determine from unique sids:
  const uniqueSids = new Set(teamOptions.map(o=>o.sid));
  const maxSize = uniqueSids.size;
  for (let k = 2; k <= maxSize; k++) sizes.push(k);

  const [sortBy, setSortBy] = useState("price"); // "teams" | "price"
  const [sortDir, setSortDir] = useState("asc");

  const combos = useMemo(()=>buildCombos(selectedSize), [selectedSize, teamOptions, overrides]);
  // v82: format team list "A & B" for 2, "A, B & C" for 3+, with last separator " & ".
  const fmtTeamList = (combo) => combo.length===2
    ? `${combo[0].team} & ${combo[1].team}`
    : combo.slice(0,-1).map(o=>o.team).join(", ") + ` & ${combo[combo.length-1].team}`;
  const rows = useMemo(()=>{
    const base = combos.map(combo => {
      const dec = combo.reduce((a,b)=>a*effDec(b), 1);
      const p = 1/dec;
      return {combo, dec, american: toAmer(p), label: fmtTeamList(combo)};
    });
    base.sort((a,b)=>{
      let cmp = 0;
      if (sortBy === "teams") cmp = a.label.localeCompare(b.label);
      else cmp = a.dec - b.dec;
      return sortDir === "asc" ? cmp : -cmp;
    });
    return base;
  }, [combos, overrides, sortBy, sortDir]);

  const fmtPrice = (r) => showDec ? r.dec.toFixed(2) : (r.american>0?`+${r.american}`:`${r.american}`);
  const fmtTeam = (opt) => {
    const d = effDec(opt);
    const p = 1/d;
    const a = toAmer(p);
    if (showDec) return `${opt.team} (${d.toFixed(2)})`;
    return `${opt.team} (${a>0?`+${a}`:a})`;
  };

  const copyTextOnly = useMemo(()=>{
    return rows.map(r => fmtTeamList(r.combo) + " to advance").join("\n");
  }, [rows]);
  const copyTextAndOdds = useMemo(()=>{
    return rows.map(r => {
      const teams = fmtTeamList(r.combo) + " to advance";
      return `${teams}\t${fmtPrice(r)}`;
    }).join("\n");
  }, [rows, showDec]);

  return <div style={{display:"grid",gridTemplateColumns:"320px minmax(0,1fr)",gap:16,alignItems:"flex-start"}}>
    {/* LEFT: matchup price overrides */}
    <Card>
      <SH title="Matchup Prices" sub={`Round: ${currentRound?.toUpperCase() || "R1"} · Edits affect only this tab`}/>
      {teamOptions.length === 0 && <div style={{fontSize:12,color:"var(--color-text-tertiary)",fontStyle:"italic"}}>No active series in this round.</div>}
      {teamOptions.length > 0 && <table style={{width:"100%",fontSize:11,borderCollapse:"collapse"}}>
        <thead>
          <tr style={{borderBottom:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-tertiary)"}}>
            <th style={{padding:"4px 4px",textAlign:"left",fontWeight:400}}>Team</th>
            <th style={{padding:"4px 4px",textAlign:"right",fontWeight:400}}>Model</th>
            <th style={{padding:"4px 4px",textAlign:"right",fontWeight:400}}>Override</th>
          </tr>
        </thead>
        <tbody>
          {teamOptions.map((opt,i) => {
            const k = `${opt.sid}|${opt.abbr}`;
            const ovr = overrides[k];
            // Group separator between series
            const prevSid = i>0 ? teamOptions[i-1].sid : null;
            const sep = prevSid && prevSid !== opt.sid;
            return <tr key={k} style={{borderTop: sep ? "0.5px solid var(--color-border-tertiary)" : "none"}}>
              <td style={{padding:"4px 4px"}}>{opt.team}</td>
              <td style={{padding:"4px 4px",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>
                {opt.modelDec.toFixed(2)}
              </td>
              <td style={{padding:"4px 4px",textAlign:"right"}}>
                <LazyNI value={ovr ?? ""} onCommit={v=>{
                  setOverrides(prev => {
                    const next = {...prev};
                    if (v === "" || v == null || isNaN(v)) delete next[k];
                    else if (v > 1) next[k] = v;
                    return next;
                  });
                }} min={1.01} max={500} step={0.01} showSpinner={false}
                  style={{width:60,padding:"2px 4px",fontSize:11,fontFamily:"var(--font-mono)",textAlign:"right",
                    background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
                    borderRadius:3,color:ovr!=null?"#fbbf24":"var(--color-text-primary)"}}/>
              </td>
            </tr>;
          })}
        </tbody>
      </table>}
      {Object.keys(overrides).length > 0 && <button
        onClick={()=>setOverrides({})}
        style={{marginTop:8,padding:"4px 10px",fontSize:10,background:"transparent",
          border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",
          borderRadius:3,cursor:"pointer"}}>
        Clear all overrides
      </button>}
    </Card>

    {/* RIGHT: parlay combos */}
    <div>
      <Card>
        <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:10,flexWrap:"wrap"}}>
          <SH title="Series Parlay Pricer" sub={`${rows.length} combos · ${selectedSize}-team`}/>
          <div style={{marginLeft:"auto",display:"flex",gap:8,alignItems:"center"}}>
            <Toggle label="Decimal" checked={showDec} onChange={setShowDec}/>
          </div>
        </div>

        {/* size selector */}
        <div style={{display:"flex",gap:0,marginBottom:10,borderRadius:"var(--border-radius-md)",overflow:"hidden",border:"0.5px solid var(--color-border-secondary)",width:"fit-content"}}>
          {sizes.map(k => <button key={k} onClick={()=>setSelectedSize(k)} style={{
            padding:"5px 12px",fontSize:11,border:"none",
            borderRight:"0.5px solid var(--color-border-tertiary)",cursor:"pointer",
            background:selectedSize===k?"#1d4ed8":"var(--color-background-secondary)",
            color:selectedSize===k?"white":"var(--color-text-secondary)"}}>
            {k} teams
          </button>)}
        </div>

        {rows.length === 0 && <div style={{fontSize:12,color:"var(--color-text-tertiary)",fontStyle:"italic",padding:"20px 0"}}>
          No valid combinations.
        </div>}

        {rows.length > 0 && <>
          <div style={{maxHeight:1100,overflowY:"auto",border:"0.5px solid var(--color-border-secondary)",borderRadius:4}}>
            <table style={{width:"100%",fontSize:11,borderCollapse:"collapse"}}>
              <thead style={{position:"sticky",top:0,background:dark?"#131625":"#fff",zIndex:1}}>
                <tr style={{borderBottom:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-tertiary)"}}>
                  <th style={{padding:"6px 8px",textAlign:"left",fontWeight:400,width:40}}>#</th>
                  <th onClick={()=>{
                    if(sortBy==="teams") setSortDir(d=>d==="asc"?"desc":"asc");
                    else { setSortBy("teams"); setSortDir("asc"); }
                  }} style={{padding:"6px 8px",textAlign:"left",fontWeight:400,cursor:"pointer",userSelect:"none"}}>
                    Teams {sortBy==="teams" ? (sortDir==="asc"?"▲":"▼") : ""}
                  </th>
                  <th onClick={()=>{
                    if(sortBy==="price") setSortDir(d=>d==="asc"?"desc":"asc");
                    else { setSortBy("price"); setSortDir("asc"); }
                  }} style={{padding:"6px 8px",textAlign:"right",fontWeight:400,width:80,cursor:"pointer",userSelect:"none"}}>
                    Price {sortBy==="price" ? (sortDir==="asc"?"▲":"▼") : ""}
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r,i)=>(
                  <tr key={i} style={{borderTop:"0.5px solid var(--color-border-tertiary)"}}>
                    <td style={{padding:"4px 8px",color:"var(--color-text-tertiary)",fontFamily:"var(--font-mono)"}}>{i+1}</td>
                    <td style={{padding:"4px 8px"}}>{r.combo.length===2 ? `${r.combo[0].team} & ${r.combo[1].team}` : r.combo.slice(0,-1).map(o=>o.team).join(", ") + ` & ${r.combo[r.combo.length-1].team}`} to advance</td>
                    <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontWeight:500}}>
                      {fmtPrice(r)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{marginTop:10,display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
            <button onClick={()=>{ navigator.clipboard?.writeText(copyTextOnly); }}
              style={{padding:"5px 12px",fontSize:11,background:"#1d4ed8",color:"white",
                border:"none",borderRadius:4,cursor:"pointer"}}>
              Copy text only
            </button>
            <button onClick={()=>{ navigator.clipboard?.writeText(copyTextAndOdds); }}
              style={{padding:"5px 12px",fontSize:11,background:"var(--color-background-secondary)",color:"var(--color-text-primary)",
                border:"0.5px solid var(--color-border-secondary)",borderRadius:4,cursor:"pointer"}}>
              Copy text & odds
            </button>
          </div>
        </>}
      </Card>
    </div>
  </div>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SETTINGS TAB
// ═══════════════════════════════════════════════════════════════════════════════
function SettingsTab({globals,setGlobals,margins,setMargins,showTrue,setShowTrue,showDec,setShowDec,doPush,doPull,doVerify,lastPushedAt,lastPulledAt,cloudInfo,syncStatus,dark}) {
  // v52: Cloud sync config (URL, key, device label) lives in localStorage via getSbConfig/setSbConfig.
  const [cfg, setCfg] = useState(() => getSbConfig());
  const [status, setStatus] = useState(null);
  const [verifyReport, setVerifyReport] = useState(null);
  const saveCfg = (patch) => { const next = {...cfg, ...patch}; setCfg(next); setSbConfig(next); };
  const pushNow = async () => {
    setStatus({kind:"info",msg:"Pushing…"});
    const r = await doPush();
    if (r.ok && r.sizes) {
      const total = Object.values(r.sizes).reduce((a,b)=>a+(b||0),0);
      setStatus({kind:"ok",msg:`Pushed at ${new Date(r.at).toLocaleTimeString()} · ${(total/1024).toFixed(1)} KB total`});
    } else {
      setStatus(r.ok ? {kind:"ok",msg:`Pushed at ${new Date(r.at).toLocaleTimeString()}`} : {kind:"err",msg:r.error});
    }
  };
  const pullNow = async () => {
    if (!window.confirm("Pull from cloud will OVERWRITE your current local state with the latest cloud copy. Continue?")) return;
    setStatus({kind:"info",msg:"Pulling…"});
    const r = await doPull();
    if (r.ok && r.diag) {
      const missing = Object.entries(r.diag).filter(([_,v])=>v.includes("missing")||v.includes("null")).map(([k])=>k);
      let msg = `Pulled from ${r.device||"?"} (${r.cloudAt?new Date(r.cloudAt).toLocaleString():"unknown time"})`;
      if (missing.length>0) msg += ` · ⚠ MISSING: ${missing.join(", ")}`;
      setStatus({kind: missing.length>0 ? "err" : "ok", msg});
      // v55: merge diag + readback into the displayed report so user can see what actually landed in localStorage
      const combined = {...r.diag};
      if (r.readback) {
        Object.entries(r.readback).forEach(([k,v]) => { combined["↳ "+k+" (local)"] = v; });
      }
      setVerifyReport(combined);
    } else {
      setStatus(r.ok ? {kind:"ok",msg:`Pulled cloud state`} : {kind:"err",msg:r.error});
    }
  };
  const verifyNow = async () => {
    setStatus({kind:"info",msg:"Verifying cloud contents…"});
    const r = await doVerify();
    if (r.ok) {
      setVerifyReport(r.report);
      setStatus({kind:"ok",msg:"Cloud inspection complete — see report below"});
    } else {
      setStatus({kind:"err",msg:r.error});
    }
  };
  const fmtTime = (iso) => {
    if (!iso) return "never";
    try { const d = new Date(iso); return d.toLocaleString(); } catch { return "?"; }
    };
  const minutesAgo = (iso) => {
    if (!iso) return null;
    try { return Math.round((Date.now() - new Date(iso).getTime()) / 60000); } catch { return null; }
  };
  const cloudNewer = cloudInfo && lastPulledAt && new Date(cloudInfo.updated_at) > new Date(lastPulledAt);
  const enabled = isSbEnabled();
  return (
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:16,alignItems:"start"}}>
      <Card>
        <SH title="Global Controls"/>
        {[{k:"overroundR1",l:"R1 Leader Overround",min:1,max:1.5,step:0.01},{k:"overroundFull",l:"Full Playoff Overround",min:1,max:1.5,step:0.01},{k:"powerFactor",l:"Power Factor",min:0.5,max:2,step:0.05},{k:"rateDiscount",l:"Rate Discount",min:0.5,max:1,step:0.01},{k:"dispersion",l:"NB Dispersion (r)",min:1,max:5,step:0.1}].map(({k,l,min,max,step})=>(
          <div key={k} style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
            <label style={{fontSize:11,color:"var(--color-text-secondary)"}}>{l}</label>
            <LazyNI value={globals[k]} onCommit={v=>setGlobals(g=>({...g,[k]:v}))} min={min} max={max} step={step}/>
          </div>
        ))}
        <div style={{marginTop:14,paddingTop:12,borderTop:"0.5px solid var(--color-border-tertiary)"}}>
          <SH title="Display"/>
          <div style={{display:"flex",flexDirection:"column",gap:8}}>
            <Toggle label="Show true probabilities" checked={showTrue} onChange={setShowTrue}/>
            <Toggle label="Show decimal odds" checked={showDec} onChange={setShowDec}/>
          </div>
        </div>
      </Card>

      <Card>
        <SH title="Market Margins"/>
        {Object.entries(margins).map(([k,v])=>(
          <div key={k} style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
            <label style={{fontSize:11,color:"var(--color-text-secondary)",textTransform:"capitalize"}}>{k.replace(/([A-Z])/g," $1")}</label>
            <LazyNI value={v} onCommit={nv=>setMargins(m=>({...m,[k]:nv}))} min={1} max={3} step={0.01} style={{width:58}}/>
          </div>
        ))}
      </Card>

      <Card>
        <SH title="Cloud Sync (Push / Pull)"/>
        {/* Config */}
        <div style={{marginBottom:10}}>
          <label style={{fontSize:10,color:"var(--color-text-secondary)",display:"block",marginBottom:2}}>Supabase URL</label>
          <input type="text" value={cfg.url||""} onChange={e=>saveCfg({url:e.target.value})} placeholder="https://xxxxx.supabase.co"
            style={{width:"100%",padding:"5px 8px",fontSize:11,fontFamily:"var(--font-mono)",background:"var(--color-background-secondary)",
              border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",boxSizing:"border-box"}}/>
        </div>
        <div style={{marginBottom:10}}>
          <label style={{fontSize:10,color:"var(--color-text-secondary)",display:"block",marginBottom:2}}>Anon Public Key</label>
          <input type="password" value={cfg.key||""} onChange={e=>saveCfg({key:e.target.value})} placeholder="eyJhbGc..."
            style={{width:"100%",padding:"5px 8px",fontSize:11,fontFamily:"var(--font-mono)",background:"var(--color-background-secondary)",
              border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",boxSizing:"border-box"}}/>
        </div>
        <div style={{marginBottom:12}}>
          <label style={{fontSize:10,color:"var(--color-text-secondary)",display:"block",marginBottom:2}}>Device Label (e.g. "laptop", "phone")</label>
          <input type="text" value={cfg.device||""} onChange={e=>saveCfg({device:e.target.value})} placeholder="laptop"
            style={{width:"100%",padding:"5px 8px",fontSize:11,background:"var(--color-background-secondary)",
              border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",boxSizing:"border-box"}}/>
        </div>

        {/* Actions */}
        <div style={{display:"flex",gap:8,marginBottom:10}}>
          <button onClick={pushNow} disabled={!enabled||syncStatus==="syncing"}
            style={{flex:1,padding:"8px 12px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
              background:enabled?"#1d4ed8":"rgba(100,116,139,0.2)",color:enabled?"#fff":"var(--color-text-tertiary)",
              cursor:enabled&&syncStatus!=="syncing"?"pointer":"not-allowed"}}>
            ⬆ Push to Cloud
          </button>
          <button onClick={pullNow} disabled={!enabled||syncStatus==="syncing"}
            style={{flex:1,padding:"8px 12px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
              background:enabled?"#7c3aed":"rgba(100,116,139,0.2)",color:enabled?"#fff":"var(--color-text-tertiary)",
              cursor:enabled&&syncStatus!=="syncing"?"pointer":"not-allowed"}}>
            ⬇ Pull from Cloud
          </button>
        </div>
        <div style={{marginBottom:10}}>
          <button onClick={verifyNow} disabled={!enabled||syncStatus==="syncing"}
            style={{width:"100%",padding:"6px 12px",fontSize:11,fontWeight:500,borderRadius:"var(--border-radius-md)",
              background:"transparent",border:`0.5px solid ${enabled?"#6b7280":"rgba(100,116,139,0.2)"}`,
              color:enabled?"var(--color-text-secondary)":"var(--color-text-tertiary)",
              cursor:enabled&&syncStatus!=="syncing"?"pointer":"not-allowed"}}>
            🔍 Verify (check what's in cloud without overwriting)
          </button>
        </div>

        {/* Verify/Pull report */}
        {verifyReport && <div style={{padding:"8px 10px",fontSize:10,borderRadius:"var(--border-radius-md)",marginBottom:8,
          background:"var(--color-background-secondary)",fontFamily:"var(--font-mono)"}}>
          <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:4,fontFamily:"var(--font-sans)",fontWeight:500}}>Cloud contents report:</div>
          {Object.entries(verifyReport).map(([k,v])=>(
            <div key={k} style={{display:"flex",gap:6,marginBottom:2,
              color:v.includes("MISSING")||v.includes("null")||v.includes("len=0")||v.includes("keys=0")?"#f87171":"var(--color-text-tertiary)"}}>
              <span style={{minWidth:80,color:"var(--color-text-secondary)"}}>{k}:</span>
              <span>{v}</span>
            </div>
          ))}
        </div>}

        {/* Status lines */}
        {status && <div style={{padding:"6px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",marginBottom:8,
          background:status.kind==="ok"?"rgba(34,197,94,0.12)":status.kind==="err"?"rgba(239,68,68,0.12)":"rgba(59,130,246,0.12)",
          color:status.kind==="ok"?"#4ade80":status.kind==="err"?"#f87171":"#60a5fa"}}>{status.msg}</div>}

        <div style={{fontSize:10,color:"var(--color-text-tertiary)",lineHeight:1.7,padding:"8px 10px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",marginBottom:10}}>
          <div><strong style={{color:"var(--color-text-secondary)"}}>Last pushed:</strong> {fmtTime(lastPushedAt)}{minutesAgo(lastPushedAt)!=null?` (${minutesAgo(lastPushedAt)}m ago)`:""}</div>
          <div><strong style={{color:"var(--color-text-secondary)"}}>Last pulled:</strong> {fmtTime(lastPulledAt)}{minutesAgo(lastPulledAt)!=null?` (${minutesAgo(lastPulledAt)}m ago)`:""}</div>
          {cloudInfo && <div><strong style={{color:"var(--color-text-secondary)"}}>Cloud latest:</strong> {fmtTime(cloudInfo.updated_at)} from <em>{cloudInfo.device||"?"}</em></div>}
          {cloudNewer && <div style={{marginTop:4,color:"#f59e0b",fontWeight:500}}>⚠ Cloud is newer than your last Pull — consider pulling</div>}
        </div>

        {/* Setup notes */}
        <details style={{marginTop:8}}>
          <summary style={{cursor:"pointer",fontSize:10,color:"var(--color-text-tertiary)"}}>One-time setup instructions</summary>
          <div style={{fontSize:10,color:"var(--color-text-tertiary)",lineHeight:1.9,padding:"8px 10px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",marginTop:6}}>
            <div style={{marginBottom:4}}>1. Create a free project at <strong>supabase.com</strong></div>
            <div style={{marginBottom:4}}>2. SQL Editor → run:</div>
            <pre style={{margin:"4px 0 8px",padding:"6px 8px",background:"var(--color-background-primary)",borderRadius:4,fontSize:9,overflowX:"auto",border:"0.5px solid var(--color-border-tertiary)"}}>
{`CREATE TABLE pricer_state (
  key TEXT PRIMARY KEY,
  value JSONB,
  device TEXT,
  updated_at TIMESTAMPTZ
);
ALTER TABLE pricer_state ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_rw" ON pricer_state
  FOR ALL TO anon USING (true) WITH CHECK (true);`}
            </pre>
            <div style={{marginBottom:4}}>3. Settings → API → copy <strong>Project URL</strong> and <strong>anon public</strong> key</div>
            <div style={{marginBottom:4}}>4. Paste into the fields above. Device label is optional — helps identify which device last pushed.</div>
            <div style={{marginTop:8,paddingTop:8,borderTop:"0.5px solid var(--color-border-tertiary)",color:"#60a5fa"}}>
              <strong>Roadmap:</strong> Auto-sync (cloud-first with conflict resolution) is a future goal once the Push/Pull workflow is validated in practice.
            </div>
          </div>
        </details>
      </Card>
    </div>
  );
}

export default AppRoot;
