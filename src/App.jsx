import { useState, useEffect, useCallback, useMemo, useRef } from "react";

// ─── SUPABASE CONFIG ─────────────────────────────────────────────────────────
const SUPABASE_URL = "";
const SUPABASE_KEY = "";
const SUPABASE_ENABLED = !!(SUPABASE_URL && SUPABASE_KEY);

async function sbLoad(key) {
  if (!SUPABASE_ENABLED) return null;
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/pricer_state?key=eq.${encodeURIComponent(key)}&select=value`, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });
    const d = await r.json();
    return d?.[0]?.value ?? null;
  } catch { return null; }
}
async function sbSave(key, value) {
  if (!SUPABASE_ENABLED) return false;
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/pricer_state`, {
      method: "POST",
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json", Prefer: "resolution=merge-duplicates" },
      body: JSON.stringify({ key, value, updated_at: new Date().toISOString() })
    });
    return true;
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
  if (r <= 1 || mu <= 0) return poissonPMF(k, mu);
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
function toAmer(p) {
  if (p <= 0.002) return 50000;
  if (p >= 0.5) return -Math.round((p / (1 - p)) * 100);
  return Math.round(((1 - p) / p) * 100);
}
function toDec(p) { return p <= 0.002 ? 501 : Math.min(501, +(1 / p).toFixed(2)); }
function applyMargin(trueProbs, or) {
  const s = trueProbs.reduce((a, b) => a + b, 0);
  return trueProbs.map(p => s > 0 ? (p / s) * or : 0);
}
function fmt(p) { const a = toAmer(p); return a > 0 ? `+${a}` : `${a}`; }

// ─── SERIES MATH ──────────────────────────────────────────────────────────────
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
function pOTGame(expTotal, winPct) {
  const lh = expTotal*winPct*0.92, la = expTotal*(1-winPct)*0.92;
  let p = 0; for (let k=0; k<=12; k++) p += poissonPMF(k,lh)*poissonPMF(k,la);
  return Math.min(p, 0.35);
}

// Series total goals: Poisson convolution over all game paths
// lambda_series = sum over paths of (sum of expTotal[g] for each game played) * P(path)
function computeSeriesGoalsLambda(effG) {
  let lambda = 0;
  function rec(gi, hw, aw, prob, goalsAcc) {
    if (hw===4||aw===4) { lambda += prob * goalsAcc; return; }
    if (gi>=7) return;
    const g = effG[gi];
    // Use actual score total if game is played and scores entered, else expTotal
    const actualTotal = (g.result && g.homeScore!=null && g.awayScore!=null)
      ? (Number(g.homeScore)+Number(g.awayScore)) : (g.expTotal || 5.5);
    const gl = actualTotal;
    if (g.result==="home") rec(gi+1, hw+1, aw, prob, goalsAcc+gl);
    else if (g.result==="away") rec(gi+1, hw, aw+1, prob, goalsAcc+gl);
    else { rec(gi+1, hw+1, aw, prob*g.winPct, goalsAcc+gl); rec(gi+1, hw, aw+1, prob*(1-g.winPct), goalsAcc+gl); }
  }
  rec(0,0,0,1,0);
  return Math.max(0.01, lambda);
}

// Series shutouts: Poisson(shutoutRate * expGames)
function computeShutoutLambda(shutoutRate, expGames) { return Math.max(0.0001, shutoutRate * expGames); }

// OT games in series: enumerate paths, P(k OT games) using Poisson per game
function computeOTSeriesDist(effG, outcomes, kMax=8) {
  // Use Poisson with lambda = sum over paths of (sum of pOT[g]) * P(path)
  let lambda = 0;
  function rec(gi, hw, aw, prob, otAcc) {
    if (hw===4||aw===4) { lambda += prob * otAcc; return; }
    if (gi>=7) return;
    const g = effG[gi];
    const pot = g.pOT ?? 0.22;
    if (g.result==="home") rec(gi+1, hw+1, aw, prob, otAcc+pot);
    else if (g.result==="away") rec(gi+1, hw, aw+1, prob, otAcc+pot);
    else { rec(gi+1, hw+1, aw, prob*g.winPct, otAcc+pot); rec(gi+1, hw, aw+1, prob*(1-g.winPct), otAcc+pot); }
  }
  rec(0,0,0,1,0);
  return { lambda: Math.max(0.0001, lambda) };
}

// Spread: home wins - away wins differential
// homeCover(line) = P(homeWins - awayWins > line) from outcomes
function computeSpread(outcomes, homeAbbr, awayAbbr) {
  // Lines from home perspective (-3.5 to -0.5) then away perspective (-1.5 to -3.5)
  const homeLines = [-3.5,-2.5,-1.5,-0.5];
  const awayLines = [-0.5,-1.5,-2.5,-3.5]; // away -0.5 = away wins series, -3.5 = away wins 4-0
  const rows = [];
  // Home favoured lines
  for(const line of homeLines){
    let pHome=0,pAway=0;
    for(const [k,prob] of Object.entries(outcomes)){
      const [hw,aw]=k.split("-").map(Number);
      if(hw-aw>line) pHome+=prob; else pAway+=prob;
    }
    rows.push({homeLabel:`${homeAbbr||"H"} ${line>0?"+":""}${line}`, awayLabel:`${awayAbbr||"A"} ${-line>0?"+":""}${-line}`, pHome, pAway, line});
  }
  // Away favoured lines (symmetric)
  for(const line of [-1.5,-2.5,-3.5]){
    let pAway=0,pHome=0;
    for(const [k,prob] of Object.entries(outcomes)){
      const [hw,aw]=k.split("-").map(Number);
      const diff=aw-hw; // away differential
      if(diff>Math.abs(line)) pAway+=prob; else pHome+=prob;
    }
    rows.push({homeLabel:`${homeAbbr||"H"} +${Math.abs(line)}`, awayLabel:`${awayAbbr||"A"} -${Math.abs(line)}`, pHome, pAway, line:null, awayLine:line});
  }
  return rows;
}

// Parlay: G1 winner x series winner (4 combos)
function computeParlays(effG, outcomes) {
  const g1wp = effG[0].winPct;
  const g1home = g1wp, g1away = 1 - g1wp;
  // If G1 already played, use actual result
  const g1result = effG[0].result;
  const g1h = g1result==="home" ? 1 : g1result==="away" ? 0 : g1home;
  const g1a = g1result==="home" ? 0 : g1result==="away" ? 1 : g1away;
  const seriesH = ["4-0","4-1","4-2","4-3"].reduce((s,k)=>s+(outcomes[k]||0),0);
  const seriesA = 1 - seriesH;
  // Approximate: joint prob (assumes G1 result slightly correlated with series)
  // Exact: enumerate with G1 constrained
  // For simplicity use independence (small error, consistent with sheet)
  return [
    { label:"Home wins G1 & wins series", tp: g1h * seriesH },
    { label:"Home loses G1 & wins series", tp: g1a * seriesH },
    { label:"Away wins G1 & wins series", tp: g1a * seriesA },
    { label:"Away loses G1 & wins series", tp: g1h * seriesA },
  ];
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

// Per-team goals O/U: use NB with each team's expected goals
// homeExpGoals = expGames * expTotal * homeWinPctAvg (approx)
function computeTeamGoalsLambda(effG, side) {
  // side: "home" or "away"
  let lam = 0;
  function rec(gi, hw, aw, prob, goalsAcc) {
    if (hw===4||aw===4) { lam += prob * goalsAcc; return; }
    if (gi>=7) return;
    const g = effG[gi];
    const total = g.expTotal || 5.5;
    // home goals ≈ total * winPct (home scoring proxy), away = total * (1-winPct)
    const contribution = side==="home" ? total * g.winPct : total * (1-g.winPct);
    if (g.result==="home") rec(gi+1, hw+1, aw, prob, goalsAcc+contribution);
    else if (g.result==="away") rec(gi+1, hw, aw+1, prob, goalsAcc+contribution);
    else { rec(gi+1, hw+1, aw, prob*g.winPct, goalsAcc+contribution); rec(gi+1, hw, aw+1, prob*(1-g.winPct), goalsAcc+contribution); }
  }
  rec(0,0,0,1,0);
  return Math.max(0.01, lam);
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
    games: Array.from({length:7},(_,i)=>({gameNum:i+1,winPct:i===0?0.55:null,expTotal:i===0?5.5:null,pOT:i===0?0.22:null,result:null})) };
}
function defaultMatchup(id) {
  return { id, homeTeam:"", awayTeam:"", homeAbbr:"", awayAbbr:"",
    homeWinPct:0.55, expTotal:5.5, homeWins:0, awayWins:0, expGames:5.82 };
}
const DEFAULT_MARGINS = {
  eightWay:1.12, winner:1.04, length:1.08, spread:1.04,
  totalGoals:1.05, winOrder:1.15, shutouts:1.05, correctScore:1.12,
  parlay:1.08, ouGames:1.05, otGames:1.08, otExact:1.08,
  teamMostGoals:1.05, teamGoals:1.05,
  propsGoals:1.08, propsAssists:1.05, propsPoints:1.05, propsSOG:1.05,
  propsHits:1.05, propsBlocks:1.05, propsTakeaways:1.05, propsPIM:1.05,
  propsGiveaways:1.05, propsTSA:1.05,
  seriesLeader:1.15, leaderR1:1.15, leaderFull:1.15,
};
const DEFAULT_GLOBALS = { overroundR1:1.15, overroundFull:1.15, powerFactor:1.0, rateDiscount:0.85, dispersion:1.2, seriesLeaderPF:0.85 };

// ─── PARSER ───────────────────────────────────────────────────────────────────
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
    TSA:["TSA","SA","ShotAtt","Attempts"], GV:["GV","Give","Giveaways","GIVE"] };
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
      tsa:cm.TSA!==undefined?parseInt(c[cm.TSA])||0:0,
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
    TOP6:"#10b981", MID6:"#64748b", BOT6:"#f59e0b", SCRATCHED:"#ef4444",
    D1:"#3b82f6", D2:"#60a5fa", D3:"#93c5fd",
    STARTER:"#a78bfa", BACKUP:"#7c3aed",
  }[r]||"#64748b";
}
// v13: All role multipliers temporarily set to 1.0 while bugs are shaken out.
// SCRATCHED still forces 0 (player is removed from pool everywhere anyway).
// STARTER/BACKUP stay 0 because goalies are excluded from skater markets by design.
// Re-attach differential weights (TOP6 1.2, BOT6 0.75, etc.) after validating upload pipeline.
function roleMultiplier(r) {
  return {TOP6:1.0, MID6:1.0, BOT6:1.0, SCRATCHED:0, D1:1.0, D2:1.0, D3:1.0, STARTER:0, BACKUP:0}[r]??1.0;
}
function RoleBadge({role}) { const c=roleColor(role||"MID6"); return <span style={{fontSize:9,padding:"1px 5px",borderRadius:3,background:`${c}20`,color:c,fontWeight:500}}>{role||"—"}</span>; }
function rolesForPos(pos) {
  if(!pos) return ["TOP6","MID6","BOT6","SCRATCHED"];
  const p=pos.toUpperCase();
  if(p==="G") return ["STARTER","BACKUP","SCRATCHED"];
  if(p==="D") return ["D1","D2","D3","SCRATCHED"];
  return ["TOP6","MID6","BOT6","SCRATCHED"];
}
function SyncBadge({status}) {
  const m={idle:["#6b7280","Offline"],syncing:["#f59e0b","Syncing…"],ok:["#10b981","Synced"],err:["#ef4444","Sync Error"]};
  const [color,label]= m[status]||m.idle;
  return <span style={{fontSize:10,padding:"2px 8px",borderRadius:10,background:`${color}20`,color,fontWeight:500}}>{label}</span>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROOT
// ═══════════════════════════════════════════════════════════════════════════════
export default function App() {
  const [dark,setDark] = useState(true);
  const [tab,setTab] = useState("leaders");
  const [globals,setGlobals] = useState(DEFAULT_GLOBALS);
  const [margins,setMargins] = useState(DEFAULT_MARGINS);
  const [showTrue,setShowTrue] = useState(false);
  const [showDec,setShowDec] = useState(true);
  const [syncStatus,setSyncStatus] = useState("idle");
  const [players,setPlayers] = useState(()=>{try{const s=localStorage.getItem("nhl_p");return s?JSON.parse(s):null;}catch{return null;}});
  const [goalies,setGoalies] = useState(()=>{try{const s=localStorage.getItem("nhl_g");return s?JSON.parse(s):null;}catch{return null;}});
  const [matchups,setMatchups] = useState(()=>{try{const s=localStorage.getItem("nhl_m");return s?JSON.parse(s):Array.from({length:8},(_,i)=>defaultMatchup(i));}catch{return Array.from({length:8},(_,i)=>defaultMatchup(i));}});
  const [advancement,setAdvancement] = useState(()=>{try{const s=localStorage.getItem("nhl_adv");return s?JSON.parse(s):PLAYOFF_TEAMS.reduce((a,t)=>({...a,[t]:{winR1:0.5,winConf:0.25,winCup:0.1}}),{});}catch{return PLAYOFF_TEAMS.reduce((a,t)=>({...a,[t]:{winR1:0.5,winConf:0.25,winCup:0.1}}),{});}});
  const [allSeries,setAllSeries] = useState(()=>{try{const s=localStorage.getItem("nhl_s");return s?JSON.parse(s):Array.from({length:8},(_,i)=>defaultSeries(i));}catch{return Array.from({length:8},(_,i)=>defaultSeries(i));}});
  const [lScope,setLScope] = useState("r1");
  const [lStat,setLStat] = useState("g");
  const [lTopN,setLTopN] = useState(25);

  useEffect(()=>{document.body.style.background=dark?"#0d0f1a":"#f1f3f7";},[dark]);
  useEffect(()=>{if(players)localStorage.setItem("nhl_p",JSON.stringify(players));},[players]);
  useEffect(()=>{if(goalies)localStorage.setItem("nhl_g",JSON.stringify(goalies));},[goalies]);
  useEffect(()=>{localStorage.setItem("nhl_m",JSON.stringify(matchups));},[matchups]);
  useEffect(()=>{localStorage.setItem("nhl_adv",JSON.stringify(advancement));},[advancement]);
  useEffect(()=>{localStorage.setItem("nhl_s",JSON.stringify(allSeries));},[allSeries]);

  const syncTimer = useRef(null);
  const pending = useRef({});
  function scheduleSync(key,val) {
    pending.current[key]=val;
    clearTimeout(syncTimer.current);
    syncTimer.current=setTimeout(async()=>{
      setSyncStatus("syncing");
      let ok=true;
      for(const[k,v] of Object.entries(pending.current)){const r=await sbSave(k,v);if(!r&&SUPABASE_ENABLED)ok=false;}
      pending.current={};
      setSyncStatus(SUPABASE_ENABLED?(ok?"ok":"err"):"idle");
    },3000);
  }
  function setP(v){setPlayers(v);scheduleSync("players",v);}
  function setG(v){setGoalies(v);scheduleSync("goalies",v);}
  function setM(v){setMatchups(v);scheduleSync("matchups",v);}
  function setAdv(v){setAdvancement(v);scheduleSync("advancement",v);}
  function setSeries(v){setAllSeries(v);scheduleSync("series",v);}

  useEffect(()=>{
    if(!SUPABASE_ENABLED)return;
    (async()=>{
      setSyncStatus("syncing");
      const keys=["players","goalies","matchups","advancement","series"];
      const setters=[setPlayers,setGoalies,setMatchups,setAdvancement,setAllSeries];
      for(let i=0;i<keys.length;i++){const v=await sbLoad(keys[i]);if(v)setters[i](v);}
      setSyncStatus("ok");
    })();
  },[]);

  const teamExpGR1 = useMemo(()=>{
    const m={};
    for(const x of matchups){if(x.homeAbbr)m[x.homeAbbr]=(m[x.homeAbbr]||0)+x.expGames;if(x.awayAbbr)m[x.awayAbbr]=(m[x.awayAbbr]||0)+x.expGames;}
    return m;
  },[matchups]);

  const computeLambda = useCallback((p,stat,scope)=>{
    const rm=roleMultiplier(p.lineRole);
    if(rm===0)return 0.0001;
    // stat key mapping: tsa->tsa_pg, give->give_pg, tk->take_pg, else stat_pg
    const pgKey=stat==="tk"?"take_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
    const actKey=stat==="tk"?"pTK":stat==="give"?"pGIVE":stat==="tsa"?"pTSA":"p"+stat.toUpperCase();
    const rr=(p[pgKey]||0)*rm*globals.rateDiscount;
    let expTotal,actualGP;
    if(scope==="r1"){expTotal=teamExpGR1[p.team]||5.82;actualGP=p.pGP||0;}
    else{const adv=advancement[p.team]||{winR1:0.5,winConf:0.25,winCup:0.1};expTotal=5.82+5.82*adv.winR1+5.82*adv.winConf+5.82*adv.winCup;actualGP=p.pGP||0;}
    return Math.max(0.0001,(p[actKey]||0)+rr*Math.max(0,expTotal-actualGP));
  },[globals.rateDiscount,teamExpGR1,advancement]);

  const leaderMarket = useMemo(()=>{
    if(!players||!players.length)return[];
    const kMax=lStat==="sog"?40:lStat==="hit"?50:lStat==="blk"?40:lStat==="tsa"?60:lStat==="give"?30:lStat==="g"&&lScope==="full"?25:20;
    const or=lScope==="r1"?globals.overroundR1:globals.overroundFull;
    const pf=globals.powerFactor;
    let pool=players.filter(p=>roleMultiplier(p.lineRole)>0);
    if(lScope==="r1"){const active=new Set([...matchups.filter(m=>m.homeAbbr).map(m=>m.homeAbbr),...matchups.filter(m=>m.awayAbbr).map(m=>m.awayAbbr)]);if(active.size>0)pool=pool.filter(p=>active.has(p.team));}
    const lambdas=pool.map(p=>computeLambda(p,lStat,lScope));
    const raw=computeLeaderProbs(lambdas,kMax);
    const powered=raw.map(p=>Math.pow(p,pf));
    const psum=powered.reduce((a,b)=>a+b,0);
    const adj=powered.map(p=>psum>0?(p/psum)*or:0);
    return pool.map((p,i)=>({...p,lambda:lambdas[i],trueProb:raw[i],adjProb:adj[i]})).sort((a,b)=>b.adjProb-a.adjProb).slice(0,lTopN);
  },[players,lStat,lScope,globals,computeLambda,matchups,advancement,lTopN]);

  function exportState(){const s={players,goalies,matchups,allSeries,advancement,globals,margins};return JSON.stringify(s,null,2);}
  function importState(text){try{const s=JSON.parse(text);if(s.players){setPlayers(s.players);scheduleSync("players",s.players);}if(s.goalies){setGoalies(s.goalies);scheduleSync("goalies",s.goalies);}if(s.matchups){setMatchups(s.matchups);}if(s.allSeries){setAllSeries(s.allSeries);}if(s.advancement){setAdvancement(s.advancement);}if(s.globals)setGlobals(s.globals);if(s.margins)setMargins(s.margins);return{ok:true};}catch(e){return{ok:false,error:e.message};}}

  const STATS=[
    {id:"g",label:"Goals"},{id:"a",label:"Assists"},{id:"pts",label:"Points"},
    {id:"sog",label:"SOG"},{id:"hit",label:"Hits"},{id:"blk",label:"Blocks"},
    {id:"tk",label:"TK"},{id:"tsa",label:"TSA"},{id:"give",label:"GV"},
  ];
  const NAV=[{id:"leaders",l:"Leader Markets"},{id:"series",l:"Series Pricer"},{id:"compare",l:"Line Compare"},{id:"upload",l:"Upload Stats"},{id:"roles",l:"Roles"},{id:"settings",l:"Settings"}];
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
        {tab==="leaders"&&<LeadersTab players={players} setPlayers={setP} matchups={matchups} setMatchups={setM}
          advancement={advancement} setAdvancement={setAdv} globals={globals} setGlobals={setGlobals}
          leaderMarket={leaderMarket} STATS={STATS} lStat={lStat} setLStat={setLStat}
          lScope={lScope} setLScope={setLScope} lTopN={lTopN} setLTopN={setLTopN}
          showTrue={showTrue} setShowTrue={setShowTrue} showDec={showDec} dark={dark}/>}
        {tab==="series"&&<SeriesTab allSeries={allSeries} setAllSeries={setSeries}
          players={players} goalies={goalies} margins={margins} setMargins={setMargins}
          globals={globals} showTrue={showTrue} dark={dark} onEnterGame={setGameModal}/>}
        {tab==="upload"&&<UploadTab players={players} setPlayers={setP} goalies={goalies} setGoalies={setG}
          exportState={exportState} importState={importState} syncStatus={syncStatus} allSeries={allSeries} dark={dark}/>}
        {tab==="compare"&&<CompareTab leaderMarket={leaderMarket} STATS={STATS} lStat={lStat} setLStat={setLStat} lScope={lScope} setLScope={setLScope} dark={dark}/>}
        {tab==="roles"&&<RolesTab players={players} setPlayers={setP} dark={dark}/>}
        {tab==="settings"&&<SettingsTab globals={globals} setGlobals={setGlobals}
          margins={margins} setMargins={setMargins}
          showTrue={showTrue} setShowTrue={setShowTrue} showDec={showDec} setShowDec={setShowDec} dark={dark}/>}
      </div>

      {gameModal!==null&&<GameEntryModal
        dark={dark}
        allSeries={allSeries}
        players={players}
        goalies={goalies}
        initialSeriesIdx={gameModal.seriesIdx}
        initialGameIdx={gameModal.gameIdx}
        onClose={()=>setGameModal(null)}
        onCommit={(seriesIdx,gameIdx,result,homeScore,awayScore,playerDeltas,goalieDeltas)=>{
          // 1. Update series game result
          setSeries(prev=>{
            const u=[...prev];
            const games=[...u[seriesIdx].games];
            games[gameIdx]={...games[gameIdx],result,homeScore,awayScore};
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
    return players.filter(p=>teams.has(p.team)&&p.lineRole!=="SCRATCHED")
      .sort((a,b)=>a.team.localeCompare(b.team)||b.pts-a.pts);
  },[players,homeAbbr,awayAbbr]);

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
    onCommit(seriesIdx,gameIdx,winner,homeScore,awayScore,pDeltas,gDeltas);
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
                <Toggle label="OT" checked={ot} onChange={setOt}/>
              </div>
            </div>
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
function LeadersTab({players,setPlayers,matchups,setMatchups,advancement,setAdvancement,globals,setGlobals,leaderMarket,STATS,lStat,setLStat,lScope,setLScope,lTopN,setLTopN,showTrue,setShowTrue,showDec,dark}) {
  const [showR1,setShowR1]=useState(false);
  const [showAdv,setShowAdv]=useState(false);
  const [filterTeam,setFilterTeam]=useState("ALL");
  const teams=[...new Set(leaderMarket.map(p=>p.team))].sort();
  const displayed=filterTeam==="ALL"?leaderMarket:leaderMarket.filter(p=>p.team===filterTeam);

  function updM(idx,f,v){setMatchups(prev=>{const u=[...prev];u[idx]={...u[idx],[f]:v};
    if(f==="homeWinPct"){const hw=v,aw=1-v;const p4=Math.pow(hw,4)+Math.pow(aw,4),p5=4*(Math.pow(hw,4)*aw+Math.pow(aw,4)*hw),p6=10*(Math.pow(hw,4)*aw*aw+Math.pow(aw,4)*hw*hw),p7=20*(Math.pow(hw,4)*aw*aw*aw+Math.pow(aw,4)*hw*hw*hw),tot=p4+p5+p6+p7;u[idx].expGames=tot>0?+((4*p4+5*p5+6*p6+7*p7)/tot).toFixed(2):5.82;}return u;});}

  return (
    <div>
      <div style={{display:"flex",gap:10,alignItems:"center",flexWrap:"wrap",marginBottom:14}}>
        <Seg options={[{id:"r1",label:"Round 1"},{id:"full",label:"Full Playoff"}]} value={lScope} onChange={setLScope}/>
        <Seg options={STATS} value={lStat} onChange={setLStat} accent="#1d4ed8"/>
        <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)} style={SEL}>
          <option value="ALL">All Teams</option>
          {teams.map(t=><option key={t} value={t}>{t} – {TEAM_NAMES[t]||t}</option>)}
        </select>
        <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center"}}>
          Top <select value={lTopN} onChange={e=>setLTopN(+e.target.value)} style={SEL}>{[10,25,50,100].map(n=><option key={n} value={n}>{n}</option>)}</select>
        </label>
        <div style={{marginLeft:"auto"}}><Toggle label="True %" checked={showTrue} onChange={setShowTrue}/></div>
      </div>

      <div style={{display:"flex",gap:14,marginBottom:12,flexWrap:"wrap",alignItems:"center",padding:"7px 12px",
        background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",border:"0.5px solid var(--color-border-tertiary)"}}>
        {[{k:"overroundR1",l:"Overround",min:1,max:1.5,step:0.01},{k:"powerFactor",l:"Power Factor",min:0.5,max:2,step:0.05},{k:"rateDiscount",l:"Rate Discount",min:0.5,max:1,step:0.01}].map(({k,l,min,max,step})=>(
          <label key={k} style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center"}}>
            {l}: <NI value={globals[k]} onChange={v=>setGlobals(g=>({...g,[k]:v}))} min={min} max={max} step={step} style={{width:56}}/>
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
                  <input key={f} placeholder={ph} value={m[f]||""} onChange={e=>updM(idx,f,f.includes("Abbr")?e.target.value.toUpperCase():e.target.value)}
                    style={{padding:"3px 6px",fontSize:11,background:"var(--color-background-primary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:3,color:"var(--color-text-primary)"}}/>
                ))}
              </div>
              <div style={{display:"flex",gap:6,fontSize:11,alignItems:"center"}}>
                <span style={{color:"var(--color-text-secondary)"}}>Win%</span>
                <NI value={m.homeWinPct} onChange={v=>updM(idx,"homeWinPct",v)} min={0} max={1} step={0.01} style={{width:52}}/>
                <span style={{color:"var(--color-text-secondary)"}}>Total</span>
                <NI value={m.expTotal} onChange={v=>updM(idx,"expTotal",v)} min={3} max={12} step={0.1} style={{width:48}}/>
                <span style={{fontSize:10,color:"var(--color-text-tertiary)",marginLeft:"auto"}}>Exp {m.expGames}g</span>
              </div>
              <div style={{display:"flex",gap:6,fontSize:11,alignItems:"center",marginTop:4}}>
                <span style={{color:"var(--color-text-secondary)"}}>Live H/A:</span>
                <NI value={m.homeWins||0} onChange={v=>updM(idx,"homeWins",Math.round(v))} min={0} max={4} step={1} style={{width:36}}/>
                <span>–</span>
                <NI value={m.awayWins||0} onChange={v=>updM(idx,"awayWins",Math.round(v))} min={0} max={4} step={1} style={{width:36}}/>
              </div>
            </div>
          ))}
        </div>
      </Card>}

      {showAdv&&lScope==="full"&&<Card style={{marginBottom:14}}>
        <SH title="Team Advancement"/>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <TH cols={["Team","P(Win R1)","P(Win Conf)","P(Win Cup)"]}/>
          <tbody>{PLAYOFF_TEAMS.map(t=>{const adv=advancement[t]||{winR1:0.5,winConf:0.25,winCup:0.1};return(
            <tr key={t} style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
              <td style={{padding:"4px 8px",fontWeight:500}}>{t} <span style={{color:"var(--color-text-secondary)",fontWeight:400}}>{TEAM_NAMES[t]}</span></td>
              {["winR1","winConf","winCup"].map(k=>(
                <td key={k} style={{padding:"3px 6px",textAlign:"right"}}>
                  <NI value={adv[k]} onChange={v=>setAdvancement(p=>({...p,[t]:{...p[t],[k]:v}}))} min={0} max={1} step={0.01} style={{width:58}}/>
                </td>
              ))}
            </tr>);})}</tbody>
        </table>
      </Card>}

      {!players?<Card style={{textAlign:"center",padding:"40px"}}>
        <div style={{fontSize:13,color:"var(--color-text-secondary)",marginBottom:6}}>No player data</div>
        <div style={{fontSize:11,color:"var(--color-text-tertiary)"}}>Upload tab → Load skaters.csv</div>
      </Card>:<Card>
        <div style={{display:"flex",gap:10,alignItems:"center",marginBottom:10}}>
          <span style={{fontSize:10,fontWeight:500,textTransform:"uppercase",letterSpacing:"0.08em",color:"var(--color-text-secondary)"}}>
            {lScope==="r1"?"R1":"Playoff"} {STATS.find(s=>s.id===lStat)?.label} Leader
          </span>
          <span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>{displayed.length} shown</span>
        </div>
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <TH cols={["#","Player","Team","Role","Now","λ",...(showTrue?["True%"]:[]),"Adj%","American",...(showDec?["Dec"]:[])]}/>
            <tbody>{displayed.map((p,i)=>{
              const rank=leaderMarket.indexOf(p)+1,a=toAmer(p.adjProb);
              const actKey=lStat==="tk"?"pTK":lStat==="give"?"pGIVE":lStat==="tsa"?"pTSA":lStat==="pim"?"pPIM":"p"+lStat.toUpperCase();
              const now=p[actKey]||0;
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
function SeriesTab({allSeries,setAllSeries,players,goalies,margins,setMargins,globals,showTrue,dark,onEnterGame}) {
  const [si,setSi]=useState(0);
  const [mkt,setMkt]=useState("winner");
  const [showMgn,setShowMgn]=useState(false);
  const s=allSeries[si];

  function updS(f,v){setAllSeries(p=>{const u=[...p];u[si]={...u[si],[f]:v};return u;});}
  function updG(gi,f,v){setAllSeries(p=>{const u=[...p],games=[...u[si].games];games[gi]={...games[gi],[f]:v};
    if(gi===0&&f==="winPct")for(let i=1;i<7;i++)if(games[i].winPct===null)games[i]={...games[i],winPct:HOME_PATTERN[i+1]?v:1-v};
    if(gi===0&&f==="expTotal")for(let i=1;i<7;i++)if(games[i].expTotal===null)games[i]={...games[i],expTotal:v};
    if(gi===0&&f==="pOT")for(let i=1;i<7;i++)if(games[i].pOT===null)games[i]={...games[i],pOT:v};
    u[si]={...u[si],games};return u;});}

  const effG=s.games.map((g,i)=>({...g,
    winPct:g.winPct??(HOME_PATTERN[i+1]?(s.games[0].winPct||0.55):1-(s.games[0].winPct||0.55)),
    expTotal:g.expTotal??(s.games[0].expTotal||5.5),
    pOT:g.pOT??(s.games[0].pOT||0.22)}));
  const effKey=JSON.stringify(effG)+JSON.stringify({sr:s.shutoutRate,wgs:s.winnerGoalShift});

  const outcomes=useMemo(()=>computeOutcomes(effG),[effKey]);
  const hwp=["4-0","4-1","4-2","4-3"].reduce((acc,k)=>acc+(outcomes[k]||0),0);
  const awp=1-hwp;
  const [adjH,adjA]=applyMargin([hwp,awp],margins.winner);

  const len4=(outcomes["4-0"]||0)+(outcomes["0-4"]||0);
  const len5=(outcomes["4-1"]||0)+(outcomes["1-4"]||0);
  const len6=(outcomes["4-2"]||0)+(outcomes["2-4"]||0);
  const len7=(outcomes["4-3"]||0)+(outcomes["3-4"]||0);
  const tot=len4+len5+len6+len7;
  const expG=tot>0?(4*len4+5*len5+6*len6+7*len7)/tot:5.82;

  const lenAdj=applyMargin([len4,len5,len6,len7],margins.length);

  const e8=useMemo(()=>{
    const rows=[{l:`${s.homeTeam||"Home"} 4-0`,k:"4-0"},{l:`${s.homeTeam||"Home"} 4-1`,k:"4-1"},{l:`${s.homeTeam||"Home"} 4-2`,k:"4-2"},{l:`${s.homeTeam||"Home"} 4-3`,k:"4-3"},{l:`${s.awayTeam||"Away"} 4-0`,k:"0-4"},{l:`${s.awayTeam||"Away"} 4-1`,k:"1-4"},{l:`${s.awayTeam||"Away"} 4-2`,k:"2-4"},{l:`${s.awayTeam||"Away"} 4-3`,k:"3-4"}].map(o=>({...o,tp:outcomes[o.k]||0}));
    const sum=rows.reduce((acc,o)=>acc+o.tp,0);
    const norm=rows.map(o=>({...o,tp:sum>0?o.tp/sum:0}));
    const adj=applyMargin(norm.map(o=>o.tp),margins.eightWay);
    return norm.map((o,i)=>({...o,ap:adj[i]}));
  },[effKey,outcomes,margins.eightWay,s.homeTeam,s.awayTeam]);

  const winOrders=useMemo(()=>{
    const seqs=computeWinOrders(effG);
    const entries=Object.entries(seqs).map(([seq,tp])=>({seq,tp,hw:seq.split("").filter(c=>c==="H").length,aw:seq.split("").filter(c=>c==="A").length}));
    const adj=applyMargin(entries.map(e=>e.tp),margins.winOrder);
    return entries.map((e,i)=>({...e,ap:adj[i]})).sort((a,b)=>b.ap-a.ap);
  },[effKey,margins.winOrder]);

  const cs3=useMemo(()=>{
    const st={};
    function rec(gi,hw,aw,prob){if(gi===3||hw===4||aw===4){const k=`${hw}-${aw}`;st[k]=(st[k]||0)+prob;return;}
      const g=effG[gi];if(g.result==="home")rec(gi+1,hw+1,aw,prob);else if(g.result==="away")rec(gi+1,hw,aw+1,prob);else{rec(gi+1,hw+1,aw,prob*g.winPct);rec(gi+1,hw,aw+1,prob*(1-g.winPct));}}
    rec(0,0,0,1);
    const entries=Object.entries(st).map(([k,tp])=>({k,tp}));
    const adj=applyMargin(entries.map(e=>e.tp),margins.correctScore);
    return entries.map((e,i)=>({...e,ap:adj[i]})).sort((a,b)=>b.tp-a.tp);
  },[effKey,margins.correctScore]);

  // Per-game OT market
  const otPerGame=useMemo(()=>effG.map((g,i)=>{
    const pOT=g.pOT??0.22;
    const [adjOT,adjNo]=applyMargin([pOT,1-pOT],margins.otGames);
    return {game:i+1,pOT,adjOT,adjNo,expTotal:g.expTotal,winPct:g.winPct};
  }),[effKey,margins.otGames]);

  // Series OT games distribution
  const otSeriesMkts=useMemo(()=>{
    const {lambda}=computeOTSeriesDist(effG,outcomes);
    const exactLines=[0,1,2,3,4,5,6,7];
    const exactProbs=exactLines.map(k=>poissonPMF(k,lambda));
    const exactAdj=applyMargin(exactProbs,margins.otExact);
    const ouLines=[0.5,1.5,2.5,3.5];
    const ouRows=ouLines.map(line=>{
      const li=Math.ceil(line-0.001);
      const pOver=1-poissonCDF(li-1,lambda);
      const [ao,au]=applyMargin([pOver,1-pOver],margins.otGames);
      return {line,pOver,pUnder:1-pOver,ao,au};
    });
    return {lambda,exactLines,exactProbs,exactAdj,ouLines,ouRows};
  },[effKey,margins.otExact,margins.otGames]);

  // Spread market
  const spreadMkt=useMemo(()=>{
    const rows=computeSpread(outcomes,s.homeAbbr,s.awayAbbr);
    return rows.map(r=>{const [ah,aa]=applyMargin([r.pHome,r.pAway],margins.spread);return {...r,ah,aa};});
  },[effKey,margins.spread,outcomes,s.homeAbbr,s.awayAbbr]);

  // Total goals O/U
  const totalGoalsMkt=useMemo(()=>{
    const lambda=computeSeriesGoalsLambda(effG);
    const autoLine=Math.round(lambda)+0.5;
    const lines=[autoLine-5,autoLine-4,autoLine-3,autoLine-2,autoLine-1,autoLine,autoLine+1,autoLine+2,autoLine+3,autoLine+4,autoLine+5].filter(l=>l>0);
    return {lambda, lines: ouTable(lambda,lines,globals.dispersion).map(r=>{const[ao,au]=applyMargin([r.pOver,r.pUnder],margins.totalGoals);return{...r,ao,au};})};
  },[effKey,margins.totalGoals,globals.dispersion]);

  // Shutouts O/U
  const shutoutMkt=useMemo(()=>{
    const rate=s.shutoutRate??0.08;
    const lambda=computeShutoutLambda(rate,expG);
    const lines=[0.5,1.5,2.5,3.5];
    return {lambda,lines:ouTable(lambda,lines).map(r=>{const[ao,au]=applyMargin([r.pOver,r.pUnder],margins.shutouts);return{...r,ao,au};})};
  },[effKey,margins.shutouts,s.shutoutRate,expG]);

  // Team most goals
  const mostGoalsMkt=useMemo(()=>{
    const shift=s.winnerGoalShift??0.15;
    const {pHomeMost,pAwayMost,pTied}=computeTeamMostGoals(hwp,awp,shift);
    const [ah,aa]=applyMargin([pHomeMost,pAwayMost],margins.teamMostGoals);
    return {pHomeMost,pAwayMost,pTied,ah,aa};
  },[effKey,margins.teamMostGoals,s.winnerGoalShift,hwp,awp]);

  // Per-team goals O/U
  const teamGoalsMkt=useMemo(()=>{
    const lamH=computeTeamGoalsLambda(effG,"home");
    const lamA=computeTeamGoalsLambda(effG,"away");
    const lineH=Math.round(lamH)+0.5, lineA=Math.round(lamA)+0.5;
    const linesH=[lineH-3,lineH-2,lineH-1,lineH,lineH+1,lineH+2,lineH+3].filter(l=>l>0);
    const linesA=[lineA-3,lineA-2,lineA-1,lineA,lineA+1,lineA+2,lineA+3].filter(l=>l>0);
    return {
      home:{lambda:lamH,rows:ouTable(lamH,linesH,globals.dispersion).map(r=>{const[ao,au]=applyMargin([r.pOver,r.pUnder],margins.teamGoals);return{...r,ao,au};})},
      away:{lambda:lamA,rows:ouTable(lamA,linesA,globals.dispersion).map(r=>{const[ao,au]=applyMargin([r.pOver,r.pUnder],margins.teamGoals);return{...r,ao,au};})},
    };
  },[effKey,margins.teamGoals,globals.dispersion]);

  // Parlays
  const parlayMkt=useMemo(()=>{
    const rows=computeParlays(effG,outcomes);
    const adj=applyMargin(rows.map(r=>r.tp),margins.parlay);
    return rows.map((r,i)=>({...r,ap:adj[i]}));
  },[effKey,margins.parlay,outcomes]);

  const MKTS=[
    {id:"winner",l:"Winner"},{id:"eightway",l:"Correct Score"},{id:"length",l:"Length"},
    {id:"spread",l:"Spread"},{id:"totalgoals",l:"Total Goals"},{id:"shutouts",l:"Shutouts"},
    {id:"winorder",l:"Win Order"},{id:"score3",l:"Score @G3"},
    {id:"ot",l:"OT/Game"},{id:"otseries",l:"OT Series"},
    {id:"mostgoals",l:"Most Goals"},{id:"teamgoals",l:"Team Goals"},
    {id:"parlay",l:"Parlay"},
    {id:"props",l:"O/U Props"},{id:"binary",l:"1+ Props"},
    {id:"goaliesaves",l:"Goalie Saves"},
    {id:"playerdetail",l:"Player Detail"},
    {id:"seriesleader",l:"Series Leader"},
  ];

  return (
    <div>
      <div style={{display:"flex",gap:6,marginBottom:14,flexWrap:"wrap",alignItems:"center"}}>
        {allSeries.map((sr,i)=><button key={i} onClick={()=>setSi(i)} style={{padding:"5px 11px",fontSize:11,borderRadius:"var(--border-radius-md)",border:"0.5px solid",cursor:"pointer",
          borderColor:si===i?"#3b82f6":"var(--color-border-secondary)",background:si===i?"#3b82f6":"var(--color-background-secondary)",color:si===i?"white":"var(--color-text-secondary)"}}>
          {sr.homeAbbr&&sr.awayAbbr?`${sr.homeAbbr} v ${sr.awayAbbr}`:`S${i+1}`}</button>)}
        <button onClick={()=>setShowMgn(v=>!v)} style={{marginLeft:"auto",padding:"4px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",cursor:"pointer",
          background:showMgn?"#1d4ed820":"var(--color-background-secondary)",border:showMgn?"0.5px solid #3b82f6":"0.5px solid var(--color-border-secondary)",
          color:showMgn?"#60a5fa":"var(--color-text-secondary)"}}>⚙ Margins</button>
      </div>

      {showMgn&&<Card style={{marginBottom:14}}>
        <SH title="Market Margins"/>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:8}}>
          {Object.entries(margins).map(([k,v])=><label key={k} style={{fontSize:11,display:"flex",justifyContent:"space-between",alignItems:"center",gap:6}}>
            <span style={{color:"var(--color-text-secondary)",textTransform:"capitalize"}}>{k.replace(/([A-Z])/g," $1")}</span>
            <NI value={v} onChange={nv=>setMargins(m=>({...m,[k]:nv}))} min={1} max={1.5} step={0.01} style={{width:56}}/>
          </label>)}
        </div>
      </Card>}

      <div style={{display:"grid",gridTemplateColumns:"310px minmax(0,1fr)",gap:14,alignItems:"start"}}>
        <div>
          <Card style={{marginBottom:10}}>
            <SH title="Series Setup"/>
            <div style={{display:"grid",gridTemplateColumns:"1fr 66px",gap:4,marginBottom:8}}>
              {[["homeTeam","Home team"],["homeAbbr","Abbr"],["awayTeam","Away team"],["awayAbbr","Abbr"]].map(([f,ph])=>(
                <input key={f} placeholder={ph} value={s[f]||""} onChange={e=>updS(f,f.includes("Abbr")?e.target.value.toUpperCase():e.target.value)}
                  style={{padding:"4px 7px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:4,color:"var(--color-text-primary)"}}/>
              ))}
            </div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                {[
                  ["G",""],
                  ["Host","Which team hosts this game (2-2-1-1-1 rotation based on series home team)"],
                  ["Host Win%","Win probability for the host team of THIS game (NOT the series home team). For a road game, this is the road team's win probability at home."],
                  ["Total","Expected total goals for this game"],
                  ["OT%","P(game goes to OT). NHL playoff avg ~22%. Affects OT markets only, not series outcome."],
                  ["Score",""],
                  ["Result",""]
                ].map(([h,tip])=><th key={h} style={{padding:"3px 3px",color:"var(--color-text-tertiary)",fontWeight:500,textAlign:"left",fontSize:9,cursor:tip?"help":"default"}} title={tip||undefined}>{h}</th>)}
              </tr></thead>
              <tbody>{effG.map((g,i)=>{
                const isHome=HOME_PATTERN[i+1];
                const homeLabel=isHome?(s.homeAbbr||"H"):(s.awayAbbr||"A");
                return (
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",opacity:g.result?0.5:1}}>
                  <td style={{padding:"2px 3px",color:"var(--color-text-tertiary)",fontSize:9}}>G{i+1}</td>
                  <td style={{padding:"2px 3px",fontSize:9,color:"var(--color-text-secondary)"}}>{homeLabel}</td>
                  <td style={{padding:"1px 2px"}}><NI value={+g.winPct.toFixed(3)} onChange={v=>updG(i,"winPct",v)} min={0} max={1} step={0.01} style={{width:46}}/></td>
                  <td style={{padding:"1px 2px"}}><NI value={+g.expTotal.toFixed(1)} onChange={v=>updG(i,"expTotal",v)} min={2} max={12} step={0.1} style={{width:40}}/></td>
                  <td style={{padding:"1px 2px"}}><NI value={+(g.pOT??0.22).toFixed(2)} onChange={v=>updG(i,"pOT",v)} min={0} max={0.5} step={0.01} style={{width:38}}/></td>
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
                  Shutout/G: <NI value={s.shutoutRate??0.08} onChange={v=>updS("shutoutRate",v)} min={0} max={0.5} step={0.01} style={{width:44}}/>
                </label>
                <label style={{color:"var(--color-text-secondary)",display:"flex",gap:4,alignItems:"center"}}>
                  Goal shift: <NI value={s.winnerGoalShift??0.15} onChange={v=>updS("winnerGoalShift",v)} min={0} max={0.4} step={0.01} style={{width:44}}/>
                </label>
                <span style={{marginLeft:"auto",color:"var(--color-text-tertiary)"}}>Exp {expG.toFixed(2)}g</span>
              </div>
              <div style={{marginTop:4,fontSize:9,color:"var(--color-text-tertiary)"}}>H:{s.games.filter(g=>g.result==="home").length} A:{s.games.filter(g=>g.result==="away").length} · TtlGoals λ {totalGoalsMkt.lambda.toFixed(1)} · Shut λ {shutoutMkt.lambda.toFixed(2)}</div>
            </div>
          </Card>
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
              {[["4g",lenAdj[0]],["5g",lenAdj[1]],["6g",lenAdj[2]],["7g",lenAdj[3]]].map(([l,ap])=>(
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

          {mkt==="winner"&&<Card><SH title="Series Winner" sub={`OR: ${margins.winner}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Team",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody><OR label={s.homeTeam||"Home"} tp={hwp} ap={adjH} showTrue={showTrue}/><OR label={s.awayTeam||"Away"} tp={awp} ap={adjA} showTrue={showTrue}/></tbody></table>
          </Card>}

          {mkt==="eightway"&&<Card><SH title="Series Correct Score" sub={`OR: ${margins.eightWay}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Outcome",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody>{e8.map((o,i)=><OR key={i} label={o.l} tp={o.tp} ap={o.ap} showTrue={showTrue}/>)}</tbody></table>
          </Card>}

          {mkt==="length"&&<Card><SH title="Series Length" sub={`OR: ${margins.length}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Games",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody>{[["4 Games",len4,lenAdj[0]],["5 Games",len5,lenAdj[1]],["6 Games",len6,lenAdj[2]],["7 Games",len7,lenAdj[3]]].map(([l,tp,ap],i)=><OR key={i} label={l} tp={tp} ap={ap} showTrue={showTrue}/>)}</tbody></table>
            <div style={{marginTop:8,padding:"5px 8px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",fontSize:11,color:"var(--color-text-secondary)"}}>
              Exp length: <strong style={{color:"var(--color-text-primary)"}}>{expG.toFixed(2)}g</strong></div>
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
                <SH title="Win Order (70-Way)" sub={`OR: ${margins.winOrder}x — sequences show game-by-game winner`}/>
                <button onClick={copyAll} style={{marginLeft:"auto",padding:"3px 10px",fontSize:10,borderRadius:"var(--border-radius-md)",background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",color:"var(--color-text-secondary)",cursor:"pointer"}}>Copy All</button>
              </div>
              <div style={{maxHeight:480,overflowY:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <TH cols={["Sequence","Winner","Games",...(showTrue?["True%"]:[]),"Adj%","American","Dec"]}/>
                  <tbody>{winOrders.map((o,i)=>(
                    <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                      <td style={{padding:"3px 8px",fontSize:10}}>{seqLabel(o.seq)}</td>
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

          {mkt==="score3"&&<Card><SH title="Correct Score After 3 Games" sub={`OR: ${margins.correctScore}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}><TH cols={["Score (H-A)",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
            <tbody>{cs3.map((o,i)=><OR key={i} label={o.k} tp={o.tp} ap={o.ap} showTrue={showTrue}/>)}</tbody></table>
          </Card>}

          {mkt==="ot"&&<Card><SH title="OT Per Game" sub={`Per-game OT probability — OR: ${margins.otGames}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Game","Home","Win%","Total","pOT","OT Adj%","OT Odds","No OT Odds"]}/>
              <tbody>{otPerGame.map((o,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                  <td style={{padding:"5px 8px",fontWeight:500}}>G{o.game}</td>
                  <td style={{padding:"5px 8px",fontSize:10,color:"var(--color-text-secondary)"}}>{HOME_PATTERN[o.game]?(s.homeAbbr||"H"):(s.awayAbbr||"A")}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.winPct*100).toFixed(0)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{o.expTotal.toFixed(1)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{(o.pOT*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11}}>{(o.adjOT*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:12,fontWeight:500}}>{fmt(o.adjOT)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(o.adjNo)}</td>
                </tr>
              ))}</tbody>
            </table>
          </Card>}

          {mkt==="otseries"&&<Card>
            <SH title="OT Games in Series" sub={`λ=${otSeriesMkts.lambda.toFixed(2)} · Exact OR: ${margins.otExact}x · O/U OR: ${margins.otGames}x`}/>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
              <div>
                <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",marginBottom:6,textTransform:"uppercase"}}>Exact # OT Games</div>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <TH cols={["#OT",...(showTrue?["True%"]:[]),"Adj%","Odds"]}/>
                  <tbody>{otSeriesMkts.exactLines.map((k,i)=><OR key={i} label={`Exactly ${k}`} tp={otSeriesMkts.exactProbs[i]} ap={otSeriesMkts.exactAdj[i]} showTrue={showTrue}/>)}</tbody>
                </table>
              </div>
              <div>
                <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",marginBottom:6,textTransform:"uppercase"}}>O/U OT Games</div>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <TH cols={["Line",...(showTrue?["Over%"]:[]),"Ov Adj%","Over","Under"]}/>
                  <tbody>{otSeriesMkts.ouRows.map((r,i)=>(
                    <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                      <td style={{padding:"4px 8px",fontFamily:"var(--font-mono)"}}>{r.line}</td>
                      {showTrue&&<td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                      <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                      <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{fmt(r.ao)}</td>
                      <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(r.au)}</td>
                    </tr>
                  ))}</tbody>
                </table>
              </div>
            </div>
          </Card>}

          {mkt==="spread"&&<Card><SH title="Series Spread" sub={`Goal differential — OR: ${margins.spread}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Home Line","Away Line",...(showTrue?["True%"]:[]),"H Adj%","H Odds","A Adj%","A Odds"]}/>
              <tbody>{spreadMkt.map((r,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.homeLabel}</td>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",color:"var(--color-text-secondary)"}}>{r.awayLabel}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pHome*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ah*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.ah>=0.5?"#4ade80":"var(--color-text-primary)"}}>{fmt(r.ah)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.aa*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.aa>=0.5?"#4ade80":"var(--color-text-primary)"}}>{fmt(r.aa)}</td>
                </tr>
              ))}</tbody>
            </table>
          </Card>}

          {mkt==="totalgoals"&&<Card><SH title="Total Goals O/U" sub={`λ=${totalGoalsMkt.lambda.toFixed(2)} goals · OR: ${margins.totalGoals}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Line",...(showTrue?["P(Over)"]:[]),"Ov Adj%","Over","Under"]}/>
              <tbody>{totalGoalsMkt.lines.map((r,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.line}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{fmt(r.ao)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(r.au)}</td>
                </tr>
              ))}</tbody>
            </table>
          </Card>}

          {mkt==="shutouts"&&<Card><SH title="Total Shutouts O/U" sub={`λ=${shutoutMkt.lambda.toFixed(3)} · Rate=${s.shutoutRate??0.08}/g · OR: ${margins.shutouts}x`}/>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Line",...(showTrue?["P(Over)"]:[]),"Ov Adj%","Over","Under"]}/>
              <tbody>{shutoutMkt.lines.map((r,i)=>(
                <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                  <td style={{padding:"5px 8px",fontFamily:"var(--font-mono)",fontWeight:500}}>{r.line}</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{fmt(r.ao)}</td>
                  <td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(r.au)}</td>
                </tr>
              ))}</tbody>
            </table>
          </Card>}

          {mkt==="mostgoals"&&<Card><SH title="Team With Most Goals" sub={`Winner goal shift: ${s.winnerGoalShift??0.15} · OR: ${margins.teamMostGoals}x`}/>
            <div style={{marginBottom:10,fontSize:11,color:"var(--color-text-tertiary)"}}>Winner outscore loser by ~{((s.winnerGoalShift??0.15)*100/2).toFixed(0)}% goal share shift. Push (tie) pays full.</div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <TH cols={["Outcome",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
              <tbody>
                <OR label={s.homeTeam||"Home"} tp={mostGoalsMkt.pHomeMost} ap={mostGoalsMkt.ah} showTrue={showTrue}/>
                <OR label={s.awayTeam||"Away"} tp={mostGoalsMkt.pAwayMost} ap={mostGoalsMkt.aa} showTrue={showTrue}/>
                <tr style={{borderBottom:"0.5px solid var(--color-border-tertiary)"}}>
                  <td style={{padding:"5px 8px",color:"var(--color-text-secondary)"}}>Tied (Push)</td>
                  {showTrue&&<td style={{padding:"5px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(mostGoalsMkt.pTied*100).toFixed(1)}%</td>}
                  <td colSpan={3} style={{padding:"5px 8px",textAlign:"right",fontSize:10,color:"var(--color-text-tertiary)"}}>void / push</td>
                </tr>
              </tbody>
            </table>
          </Card>}

          {mkt==="teamgoals"&&<Card><SH title="Per-Team Total Goals O/U" sub={`OR: ${margins.teamGoals}x`}/>
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
                      <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
                        <td style={{padding:"4px 6px",fontFamily:"var(--font-mono)"}}>{r.line}</td>
                        {showTrue&&<td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>}
                        <td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                        <td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500}}>{fmt(r.ao)}</td>
                        <td style={{padding:"4px 6px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(r.au)}</td>
                      </tr>
                    ))}</tbody>
                  </table>
                </div>
              ))}
            </div>
          </Card>}

          {mkt==="parlay"&&(()=>{
            const gPlayed=s.games.filter(g=>g.result).length;
            const parlayGame=gPlayed+1;
            const parlayTitle=gPlayed===0?`Game 1 × Series Winner Parlay`:`Game ${parlayGame} × Series Winner Parlay`;
            const parlayNote=gPlayed===0
              ?"G1 result × series winner (4 combos)"
              :`Based on current series state (${s.games.filter(g=>g.result==="home").length}-${s.games.filter(g=>g.result==="away").length}). Next game × series winner.`;
            return <Card>
              <SH title={parlayTitle} sub={`OR: ${margins.parlay}x`}/>
              <div style={{marginBottom:8,fontSize:11,color:"var(--color-text-tertiary)"}}>{parlayNote}</div>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <TH cols={["Parlay",...(showTrue?["True%"]:[]),"Adj%","American","Decimal"]}/>
                <tbody>{parlayMkt.map((r,i)=><OR key={i} label={r.label.replace("Home",s.homeTeam||"Home").replace("Away",s.awayTeam||"Away")} tp={r.tp} ap={r.ap} showTrue={showTrue}/>)}</tbody>
              </table>
            </Card>;
          })()}

          {mkt==="props"&&<PropsPanel s={s} expG={expG} players={players} globals={globals} margins={margins} showTrue={showTrue} dark={dark} mode="ou"/>}
          {mkt==="binary"&&<PropsPanel s={s} expG={expG} players={players} globals={globals} margins={margins} showTrue={showTrue} dark={dark} mode="binary"/>}
          {mkt==="goaliesaves"&&<GoalieSavesPanel s={s} expG={expG} goalies={goalies} margins={margins} showTrue={showTrue} dark={dark}/>}
          {mkt==="playerdetail"&&<PlayerDetailPanel s={s} expG={expG} players={players} globals={globals} margins={margins} showTrue={showTrue} dark={dark}/>}
          {mkt==="seriesleader"&&<SeriesLeaderPanel s={s} expG={expG} players={players} globals={globals} margins={margins} showTrue={showTrue} dark={dark}/>}
        </div>
      </div>
    </div>
  );
}

// ─── PROPS PANEL (shared O/U + Binary) ───────────────────────────────────────
function PropsPanel({s,expG,players,globals,margins,showTrue,dark,mode}) {
  const [stat,setStat]=useState("g");
  const [line,setLine]=useState(mode==="binary"?1:0.5);
  const STATS=[
    {id:"g",label:"Goals",mk:"propsGoals"},{id:"a",label:"Assists",mk:"propsAssists"},
    {id:"pts",label:"Points",mk:"propsPoints"},{id:"sog",label:"SOG",mk:"propsSOG"},
    {id:"hit",label:"Hits",mk:"propsHits"},{id:"blk",label:"Blocks",mk:"propsBlocks"},
    {id:"tk",label:"Takeaways",mk:"propsTakeaways"},{id:"pim",label:"PIM",mk:"propsPIM"},
    {id:"give",label:"Giveaways",mk:"propsGiveaways"},{id:"tsa",label:"TSA",mk:"propsTSA"},
  ];
  const statMeta=STATS.find(x=>x.id===stat)||STATS[0];
  const statMargin=margins[statMeta.mk]||1.05;

  const pool=useMemo(()=>{
    if(!players)return[];
    const teams=new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    return players.filter(p=>teams.has(p.team)&&p.lineRole!=="SCRATCHED");
  },[players,s.homeAbbr,s.awayAbbr]);

  const results=useMemo(()=>{
    const {rateDiscount,dispersion}=globals;
    return pool.map(p=>{
      const rm=roleMultiplier(p.lineRole);
      // stat key mapping
      const pgKey=stat==="tk"?"take_pg":stat==="pim"?"pim_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
      const actKey=stat==="tk"?"pTK":stat==="pim"?"pPIM":stat==="give"?"pGIVE":stat==="tsa"?"pTSA":"p"+stat.toUpperCase();
      const rr=(p[pgKey]||0)*rm*rateDiscount;
      const lam=Math.max(0.0001,(p[actKey]||0)+rr*Math.max(0,expG-(p.pGP||0)));
      const actual=p[actKey]||0;
      // For binary: if player already has >= line, they've hit it — price at near-certainty
      const effectiveLine = mode==="binary" ? Math.max(line, actual+1) : line;
      const lineInt=Math.ceil(effectiveLine-0.001);
      const pOver=mode==="binary"&&actual>=line ? 1 : 1-nbCDF(lineInt-1,lam,dispersion);
      const [adjO,adjU]=applyMargin([pOver,1-pOver],statMargin);
      return {...p,lam,pYes:pOver,adjYes:adjO,adjNo:adjU};
    }).sort((a,b)=>b.adjYes-a.adjYes);
  },[pool,stat,line,globals,expG,statMargin]);

  if(!s.homeAbbr||!s.awayAbbr) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Set team abbreviations to load props</div></Card>;

  const title=mode==="binary"
    ?`To Record ${line}+ ${statMeta.label}`
    :`O/U ${statMeta.label} — Line ${line}`;

  return <Card>
    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:10,flexWrap:"wrap"}}>
      <SH title={title} sub={`OR: ${statMargin}x · λ from reg season × ${globals.rateDiscount} discount`}/>
      <Seg options={STATS.map(s=>({id:s.id,label:s.label}))} value={stat} onChange={v=>{setStat(v);setLine(mode==="binary"?1:v==="sog"?2.5:v==="hit"?2.5:0.5);}} accent="#1d4ed8"/>
      <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:5,alignItems:"center"}}>
        {mode==="binary"?"Min:":"Line:"} <NI value={line} onChange={setLine} min={mode==="binary"?1:0.5} max={50} step={mode==="binary"?1:0.5} style={{width:48}}/>
      </label>
    </div>
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
        <TH cols={["#","Player","Team","Role","λ",...(showTrue?["True%"]:[]),"Adj%","Yes Odds","No Odds"]}/>
        <tbody>{results.map((p,i)=>(
          <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
            <td style={{padding:"3px 8px",color:"var(--color-text-tertiary)",fontSize:9}}>{i+1}</td>
            <td style={{padding:"3px 8px",fontWeight:i<3?500:400}}>{p.name}</td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><RoleBadge role={p.lineRole}/></td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{p.lam.toFixed(2)}</td>
            {showTrue&&<td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(p.pYes*100).toFixed(1)}%</td>}
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(p.adjYes*100).toFixed(1)}%</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:p.adjYes>=0.5?"#4ade80":"var(--color-text-primary)"}}>{fmt(p.adjYes)}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(p.adjNo)}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  </Card>;
}

// ─── GOALIE SAVES PANEL ──────────────────────────────────────────────────────
// Model: lambda = starter_share × saves_pg × expGames (Poisson O/U)
// Line auto-set to round(lambda) - 0.5 (nearest under). Matches Goalie Series Props sheet.
function GoalieSavesPanel({s,expG,goalies,margins,showTrue,dark}) {
  const or = margins.propsGoals||1.05; // reuse saves margin setting

  const seriesGoalies = useMemo(()=>{
    if(!goalies) return [];
    const teams = new Set([s.homeAbbr, s.awayAbbr].filter(Boolean));
    if(!teams.size) return [];
    return goalies
      .filter(g => teams.has(g.team))
      .map(g => {
        const lam = Math.max(0.0001, g.starter_share * g.saves_pg * expG);
        const autoLine = Math.max(0.5, Math.round(lam) - 0.5);
        return {...g, lam, autoLine};
      })
      .sort((a,b) => b.starter_share - a.starter_share);
  },[goalies, s.homeAbbr, s.awayAbbr, expG]);

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
function PlayerDetailPanel({s,expG,players,globals,margins,showTrue,dark}) {
  const [selectedPlayer,setSelectedPlayer] = useState("");
  const [stat,setStat] = useState("g");
  const STATS=[
    {id:"g",label:"Goals",mk:"propsGoals"},{id:"a",label:"Assists",mk:"propsAssists"},
    {id:"pts",label:"Points",mk:"propsPoints"},{id:"sog",label:"SOG",mk:"propsSOG"},
    {id:"hit",label:"Hits",mk:"propsHits"},{id:"blk",label:"Blocks",mk:"propsBlocks"},
    {id:"tk",label:"Takeaways",mk:"propsTakeaways"},{id:"pim",label:"PIM",mk:"propsPIM"},
    {id:"give",label:"Giveaways",mk:"propsGiveaways"},{id:"tsa",label:"TSA",mk:"propsTSA"},
  ];
  const statMeta = STATS.find(x=>x.id===stat)||STATS[0];
  const or = margins[statMeta.mk]||1.05;

  const pool = useMemo(()=>{
    if(!players) return [];
    const teams = new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    return players.filter(p=>teams.has(p.team)&&p.lineRole!=="SCRATCHED").sort((a,b)=>a.team.localeCompare(b.team)||b.pts-a.pts);
  },[players,s.homeAbbr,s.awayAbbr]);

  const player = pool.find(p=>p.name===selectedPlayer);

  const lines = useMemo(()=>{
    if(!player) return [];
    const {rateDiscount,dispersion} = globals;
    const rm = roleMultiplier(player.lineRole);
    const pgKey = stat==="tk"?"take_pg":stat==="pim"?"pim_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat+"_pg";
    const actKey = stat==="tk"?"pTK":stat==="pim"?"pPIM":stat==="give"?"pGIVE":stat==="tsa"?"pTSA":"p"+stat.toUpperCase();
    const rr = (player[pgKey]||0)*rm*rateDiscount;
    const lam = Math.max(0.0001,(player[actKey]||0)+rr*Math.max(0,expG-(player.pGP||0)));
    // Build line table: 0.5 through ceil(lam*2)+2, step 0.5
    const maxLine = Math.ceil(lam)+4;
    const lineArr = [];
    for(let l=0.5; l<=maxLine; l+=0.5) {
      const li = Math.ceil(l-0.001);
      const pOver = 1-nbCDF(li-1,lam,dispersion);
      const pUnder = 1-pOver;
      const [ao,au] = applyMargin([pOver,pUnder],or);
      // Need = how many more to go over
      const actual = player[actKey]||0;
      const need = Math.max(0, li-actual);
      lineArr.push({line:l,pOver,pUnder,ao,au,lam,actual,need});
    }
    return {lam, rows: lineArr};
  },[player,stat,expG,globals,or]);

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
            // Highlight the auto-line row (closest to 50/50 or round number)
            const isKey = Math.abs(r.pOver-0.5)<0.08;
            return (
              <tr key={i} style={{
                borderBottom:"0.5px solid var(--color-border-tertiary)",
                background: isKey?(dark?"rgba(59,130,246,0.08)":"rgba(59,130,246,0.05)"):i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)"),
              }}>
                <td style={{padding:"3px 8px",fontFamily:"var(--font-mono)",fontWeight:isKey?500:400,color:isKey?"#60a5fa":"var(--color-text-primary)"}}>{r.line.toFixed(1)}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{r.actual}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:r.need===0?"#4ade80":"var(--color-text-secondary)"}}>{r.need===0?"✓":r.need}</td>
                {showTrue&&<>
                  <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pOver*100).toFixed(1)}%</td>
                  <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.pUnder*100).toFixed(1)}%</td>
                </>}
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(r.ao*100).toFixed(1)}%</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(r.au*100).toFixed(1)}%</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:500,color:r.ao>=0.5?"#4ade80":"var(--color-text-primary)"}}>{fmt(r.ao)}</td>
                <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,color:"var(--color-text-secondary)"}}>{fmt(r.au)}</td>
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
  {id:"sog",  label:"SOG",       temp:2.0, kMax:40},
  {id:"hit",  label:"Hits",      temp:1.5, kMax:50},
  {id:"blk",  label:"Blocks",    temp:1.5, kMax:40},
  {id:"tk",   label:"TK",        temp:1.5, kMax:20},
  {id:"give", label:"GV",        temp:1.5, kMax:30},
  {id:"pim",  label:"PIM",       temp:1.5, kMax:25},
  {id:"tsa",  label:"TSA",       temp:2.0, kMax:60},
];

function SeriesLeaderPanel({s,expG,players,globals,margins,showTrue,dark}) {
  const [stat,setStat]=useState("g");
  const [customTemps,setCustomTemps]=useState({});
  const meta=SERIES_LEADER_STATS.find(x=>x.id===stat)||SERIES_LEADER_STATS[0];
  const temp=customTemps[stat]??meta.temp;
  const or=margins.seriesLeader||1.15;

  const pool=useMemo(()=>{
    if(!players)return[];
    const teams=new Set([s.homeAbbr,s.awayAbbr].filter(Boolean));
    return players.filter(p=>teams.has(p.team)&&p.lineRole!=="SCRATCHED");
  },[players,s.homeAbbr,s.awayAbbr]);

  const leaderRows=useMemo(()=>{
    if(!pool.length)return[];
    const {rateDiscount}=globals;
    const lambdas=pool.map(p=>{
      const rm=roleMultiplier(p.lineRole);
      const pgKey=stat==="tk"?"take_pg":stat==="give"?"give_pg":stat==="tsa"?"tsa_pg":stat==="pim"?"pim_pg":stat+"_pg";
      const actKey=stat==="tk"?"pTK":stat==="give"?"pGIVE":stat==="tsa"?"pTSA":stat==="pim"?"pPIM":"p"+stat.toUpperCase();
      const rr=(p[pgKey]||0)*rm*rateDiscount;
      return Math.max(0.0001,(p[actKey]||0)+rr*Math.max(0,expG-(p.pGP||0)));
    });
    const raw=computeLeaderProbs(lambdas,meta.kMax);
    // Apply per-stat temperature (power factor) then normalize with overround
    const powered=raw.map(p=>Math.pow(p,temp));
    const psum=powered.reduce((a,b)=>a+b,0);
    const adj=powered.map(p=>psum>0?(p/psum)*or:0);
    return pool.map((p,i)=>({...p,lambda:lambdas[i],trueProb:raw[i],adjProb:adj[i]}))
      .sort((a,b)=>b.adjProb-a.adjProb);
  },[pool,stat,expG,globals,temp,or,meta.kMax]);

  if(!s.homeAbbr||!s.awayAbbr) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12}}>Set team abbreviations to load series leaders</div></Card>;

  return <Card>
    <div style={{display:"flex",gap:8,alignItems:"center",marginBottom:10,flexWrap:"wrap"}}>
      <SH title="Series Stat Leader" sub={`Dead-heat · OR: ${or}x · Temp: ${temp}`}/>
      <Seg options={SERIES_LEADER_STATS.map(s=>({id:s.id,label:s.label}))} value={stat} onChange={setStat} accent="#7c3aed"/>
      <label style={{fontSize:11,color:"var(--color-text-secondary)",display:"flex",gap:4,alignItems:"center"}}>
        Temp: <NI value={temp} onChange={v=>setCustomTemps(t=>({...t,[stat]:v}))} min={0.5} max={5} step={0.1} style={{width:46}}/>
      </label>
    </div>
    <div style={{overflowX:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
        <TH cols={["#","Player","Team","Role","Now","λ",...(showTrue?["True%","DH%"]:[]),"Adj%","American","Dec"]}/>
        <tbody>{leaderRows.map((p,i)=>{
          const a=toAmer(p.adjProb);
          // current actual series stat
          const actKey=stat==="tk"?"pTK":stat==="give"?"pGIVE":stat==="tsa"?"pTSA":stat==="pim"?"pPIM":"p"+stat.toUpperCase();
          const now=p[actKey]||0;
          return <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",background:i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
            <td style={{padding:"3px 8px",color:"var(--color-text-tertiary)",fontSize:9}}>{i+1}</td>
            <td style={{padding:"3px 8px",fontWeight:i<3?500:400}}>{p.name}</td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(124,58,237,0.15)",color:"#a78bfa"}}>{p.team}</span></td>
            <td style={{padding:"3px 8px",textAlign:"right"}}><RoleBadge role={p.lineRole}/></td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:11,fontWeight:now>0?500:400,color:now>0?"#4ade80":"var(--color-text-tertiary)"}}>{now>0?now:"—"}</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{p.lambda.toFixed(2)}</td>
            {showTrue&&<><td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(p.trueProb*100).toFixed(2)}%</td>
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{(Math.pow(p.trueProb,temp)*100).toFixed(2)}%</td></>}
            <td style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10}}>{(p.adjProb*100).toFixed(2)}%</td>
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
          {[{id:"r1",label:"Round 1"},{id:"full",label:"Full Playoff"}].map(s=>(
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
          <SH title={`${lScope==="r1"?"R1":"Playoff"} ${STATS.find(s=>s.id===lStat)?.label||""} — ${activeBook} vs Our Price`}
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
// Paste an HR box score for one team + one game → increments player playoff totals
function GameStatImporter({players,setPlayers,allSeries}) {
  const [seriesIdx,setSeriesIdx]=useState(0);
  const [gameNum,setGameNum]=useState(1);
  const [paste,setPaste]=useState("");
  const [result,setResult]=useState(null);
  const [err,setErr]=useState("");
  const [log,setLog]=useState([]); // upload history

  const s=allSeries?.[seriesIdx];
  const seriesLabel=(sr)=>sr.homeAbbr&&sr.awayAbbr?`${sr.homeAbbr} vs ${sr.awayAbbr}`:`Series ${sr._idx+1}`;

  // Parse HR skater box score — tab-separated, detects column positions
  function parseHRGame(text){
    const lines=text.trim().split("\n");
    let hi=-1,headers=[];
    for(let i=0;i<lines.length;i++){
      const c=lines[i].split("\t").map(s=>s.trim());
      if(c.some(h=>["Player","Skater","Name"].includes(h))){hi=i;headers=c;break;}
    }
    if(hi===-1) return {error:"No header row found — copy the full table including headers"};
    // Map columns
    const col=(alts)=>{for(const a of alts){const i=headers.findIndex(h=>h.toLowerCase()===a.toLowerCase());if(i!==-1)return i;}return -1;};
    const cm={
      name:col(["Player","Skater","Name"]),
      g:col(["G","Goals"]),
      a:col(["A","Assists"]),
      sog:col(["S","SOG","Shots"]),
      pim:col(["PIM"]),
      // HR game box score doesn't have HIT/BLK/TK — those aren't in the standard scoring table
    };
    if(cm.name===-1) return {error:"Could not find Player column"};
    const players=[];
    for(let i=hi+1;i<lines.length;i++){
      const c=lines[i].split("\t").map(s=>s.trim());
      const name=cm.name!==-1?c[cm.name]:"";
      if(!name||["Player","Rk","TOTAL","Team Totals"].some(x=>name.includes(x))) continue;
      players.push({
        name,
        g:cm.g!==-1?parseInt(c[cm.g])||0:0,
        a:cm.a!==-1?parseInt(c[cm.a])||0:0,
        sog:cm.sog!==-1?parseInt(c[cm.sog])||0:0,
        pim:cm.pim!==-1?parseInt(c[cm.pim])||0:0,
      });
    }
    return {players};
  }

  function handleImport(){
    setErr("");setResult(null);
    if(!players){setErr("Load skaters.csv first.");return;}
    if(!paste.trim()){return;}
    const r=parseHRGame(paste);
    if(r.error){setErr(r.error);return;}

    // Normalize name for matching
    const norm=s=>s.toLowerCase().replace(/[^a-z]/g,"");
    let matched=0;const unmatched=[];

    const updated=players.map(p=>{
      const m=r.players.find(u=>
        norm(u.name)===norm(p.name)||
        // last name fallback
        (norm(u.name).length>=4&&norm(p.name).endsWith(norm(u.name.split(" ").pop()||"")))
      );
      if(m){
        matched++;
        return{...p,
          pGP:(p.pGP||0)+1,
          pG:(p.pG||0)+m.g,
          pA:(p.pA||0)+m.a,
          pSOG:(p.pSOG||0)+m.sog,
          pPIM:(p.pPIM||0)+m.pim,
        };
      }
      return p;
    });

    // Report unmatched HR names
    r.players.forEach(u=>{
      const m=players.find(p=>norm(u.name)===norm(p.name));
      if(!m) unmatched.push(u.name);
    });

    setPlayers(updated);
    const entry={ts:new Date().toLocaleTimeString(),matched,series:s?`${s.homeAbbr||"?"}/${s.awayAbbr||"?"}`:"-",game:gameNum,unmatched:unmatched.slice(0,5)};
    setLog(prev=>[entry,...prev].slice(0,20));
    setResult({matched,unmatched:unmatched.slice(0,10),gameNum,seriesLabel:s?`${s.homeAbbr||"?"} vs ${s.awayAbbr||"?"}`:""});
    setPaste("");
  }

  return <div>
    {/* Series + Game selector */}
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginBottom:10}}>
      <div>
        <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:4}}>Series</div>
        <select value={seriesIdx} onChange={e=>setSeriesIdx(+e.target.value)}
          style={{width:"100%",padding:"5px 8px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)"}}>
          {(allSeries||[]).map((sr,i)=><option key={i} value={i}>{sr.homeAbbr&&sr.awayAbbr?`${sr.homeAbbr} vs ${sr.awayAbbr}`:`Series ${i+1}`}</option>)}
        </select>
      </div>
      <div>
        <div style={{fontSize:10,color:"var(--color-text-secondary)",marginBottom:4}}>Game</div>
        <select value={gameNum} onChange={e=>setGameNum(+e.target.value)}
          style={{width:"100%",padding:"5px 8px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)"}}>
          {[1,2,3,4,5,6,7].map(n=><option key={n} value={n}>Game {n}</option>)}
        </select>
      </div>
    </div>

    <div style={{fontSize:10,color:"var(--color-text-tertiary)",marginBottom:8}}>
      Paste the HR box score for <strong>one team</strong> at a time. Stats are <strong>added</strong> to running playoff totals (G, A, SOG, PIM incremented by +1 game each paste). Repeat for the other team.
    </div>

    <textarea value={paste} onChange={e=>setPaste(e.target.value)}
      placeholder={"Paste HR box score here (tab-separated with headers)…\n\nExample:\nPlayer\tG\tA\tPTS\t+/-\tPIM\t…\tS\nSidney Crosby\t1\t1\t2\t+1\t0\t…\t4\n\nHR columns mapped: Player, G, A, S (shots), PIM\nHIT/BLK/TK not available in HR box score — enter via game entry modal"}
      style={{width:"100%",height:180,fontSize:11,fontFamily:"var(--font-mono)",
        background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
        borderRadius:"var(--border-radius-md)",padding:10,color:"var(--color-text-primary)",
        resize:"vertical",boxSizing:"border-box",marginBottom:8}}/>

    <div style={{display:"flex",gap:8,alignItems:"center"}}>
      <button onClick={handleImport} disabled={!paste.trim()||!players}
        style={{padding:"6px 18px",fontSize:12,fontWeight:500,borderRadius:"var(--border-radius-md)",border:"none",
          cursor:paste.trim()&&players?"pointer":"default",
          background:paste.trim()&&players?"#10b981":"var(--color-background-secondary)",
          color:paste.trim()&&players?"white":"var(--color-text-tertiary)"}}>
        + Add to Totals
      </button>
      <button onClick={()=>{setPaste("");setResult(null);setErr("");}}
        style={{padding:"5px 10px",fontSize:11,borderRadius:"var(--border-radius-md)",
          background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",
          color:"var(--color-text-secondary)",cursor:"pointer"}}>Clear</button>
      {!players&&<span style={{fontSize:11,color:"#f59e0b"}}>Load skaters.csv first</span>}
    </div>

    {err&&<div style={{marginTop:8,padding:8,borderRadius:"var(--border-radius-md)",
      background:"rgba(239,68,68,0.1)",border:"0.5px solid rgba(239,68,68,0.3)",fontSize:11,color:"#ef4444"}}>{err}</div>}
    {result&&<div style={{marginTop:8,padding:8,borderRadius:"var(--border-radius-md)",
      background:"rgba(16,185,129,0.1)",border:"0.5px solid rgba(16,185,129,0.3)",fontSize:11}}>
      <div style={{color:"#10b981",fontWeight:500,marginBottom:2}}>
        ✓ {result.matched} players updated — {result.seriesLabel} G{result.gameNum}
      </div>
      <div style={{fontSize:10,color:"var(--color-text-secondary)"}}>pGP+1, pG/pA/pSOG/pPIM incremented</div>
      {result.unmatched.length>0&&<div style={{fontSize:10,color:"#f59e0b",marginTop:4}}>
        Not in roster: {result.unmatched.join(", ")}{result.unmatched.length>=10?"…":""}
      </div>}
    </div>}
    {log.length>0&&<div style={{marginTop:10}}>
      <div style={{fontSize:10,fontWeight:500,color:"var(--color-text-secondary)",marginBottom:4,textTransform:"uppercase",letterSpacing:"0.06em"}}>Upload Log</div>
      <div style={{maxHeight:120,overflowY:"auto"}}>
        {log.map((e,i)=><div key={i} style={{display:"flex",gap:10,fontSize:10,padding:"3px 0",borderBottom:"0.5px solid var(--color-border-tertiary)",color:i===0?"var(--color-text-primary)":"var(--color-text-secondary)"}}>
          <span style={{color:"var(--color-text-tertiary)",flexShrink:0}}>{e.ts}</span>
          <span style={{color:"#10b981",flexShrink:0}}>✓ {e.matched}p</span>
          <span style={{flexShrink:0}}>{e.series} G{e.game}</span>
          {e.unmatched.length>0&&<span style={{color:"#f59e0b"}}>⚠ {e.unmatched.join(", ")}</span>}
        </div>)}
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
            {["Player","Team","GP","G","A","Pts","SOG","PIM"].map(h=>(
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
            <td style={{padding:"2px 6px",textAlign:"right",fontFamily:"var(--font-mono)",color:"var(--color-text-tertiary)"}}>{p.pPIM||0}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  </div>;
}

// ═══════════════════════════════════════════════════════════════════════════════
// UPLOAD TAB
// ═══════════════════════════════════════════════════════════════════════════════
function UploadTab({players,setPlayers,goalies,setGoalies,exportState,importState,syncStatus,allSeries,dark}) {
  const [fileErr,setFileErr]=useState("");
  const setErr=setFileErr; // alias used in file handlers

  const playerFileRef = useRef(null);
  const goalieFileRef = useRef(null);

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
          const posVal=c[idx("position")]?.trim()||"F";
          const name=c[idx("name")]?.trim();if(!name)continue;
          const defRole=posVal==="D"?"D2":posVal==="G"?"BACKUP":"MID6";
          parsed.push({name,team,pos:posVal,gp:Math.round(gp),g,a,pts:g+a,sog,hit,blk,tk,pim,tsa,give,
            g_pg:+(g/gp).toFixed(4),a_pg:+(a/gp).toFixed(4),pts_pg:+((g+a)/gp).toFixed(4),
            sog_pg:+(sog/gp).toFixed(4),hit_pg:+(hit/gp).toFixed(4),blk_pg:+(blk/gp).toFixed(4),
            take_pg:+(tk/gp).toFixed(4),pim_pg:+(pim/gp).toFixed(4),
            tsa_pg:+(tsa/gp).toFixed(4),give_pg:+(give/gp).toFixed(4),
            lineRole:defRole,pGP:0,pG:0,pA:0,pSOG:0,pHIT:0,pBLK:0,pTK:0,pPIM:0,pTSA:0,pGIVE:0});
        }
        parsed.sort((a,b)=>a.team.localeCompare(b.team)||b.pts-a.pts);
        setPlayers(parsed);
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
          const saves=ongoal-goals;
          const name=c[idx("name")]?.trim();if(!name)continue;
          rawGoalies.push({name,team,gp:Math.round(gp),saves,saves_pg:+(saves/gp).toFixed(4)});
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

  return (
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:20,alignItems:"start"}}>
      {/* Hidden real file inputs — triggered by visible buttons */}
      <input ref={playerFileRef} type="file" accept=".csv,.json" onChange={handlePlayerFile} style={{display:"none"}}/>
      <input ref={goalieFileRef} type="file" accept=".csv,.json" onChange={handleGoalieFile} style={{display:"none"}}/>
      <div>
        <Card style={{marginBottom:14}}>
          <SH title="Game Stat Import" sub="Paste a Hockey Reference box score — stats are added to each player's running playoff totals"/>
          {/* Series + Game selector */}
          <GameStatImporter players={players} setPlayers={setPlayers} allSeries={allSeries}/>
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
          <button onClick={()=>playerFileRef.current?.click()} style={{padding:"6px 16px",fontSize:12,borderRadius:"var(--border-radius-md)",background:"#3b82f6",color:"white",border:"none",cursor:"pointer"}}>{players?"Re-load skaters.csv":"Load skaters.csv"}</button>
          {players&&<div style={{marginTop:10,display:"flex",gap:4,flexWrap:"wrap"}}>
            {["TOP6","MID6","BOT6","D1","D2","D3","STARTER","BACKUP","SCRATCHED"].map(r=>{
              const cnt=players.filter(p=>p.lineRole===r).length;
              if(!cnt) return null;
              const c=roleColor(r);
              return <div key={r} style={{fontSize:9,padding:"2px 7px",borderRadius:3,background:`${c}20`,color:c,fontWeight:500}}>{r}: {cnt}</div>;
            })}
          </div>}
          {fileErr&&<div style={{marginTop:8,fontSize:11,color:"#ef4444"}}>{fileErr}</div>}
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
          <SH title="Supabase Sync"/>
          <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:8}}><SyncBadge status={syncStatus}/><span style={{fontSize:10,color:"var(--color-text-tertiary)"}}>{SUPABASE_ENABLED?"Auto-syncing":"Not configured"}</span></div>
          <div style={{fontSize:9,color:"var(--color-text-tertiary)",padding:"6px 8px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",fontFamily:"var(--font-mono)"}}>
            CREATE TABLE pricer_state (key TEXT PRIMARY KEY, value JSONB, updated_at TIMESTAMPTZ);
          </div>
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
function RolesTab({players,setPlayers,dark}) {
  const [filterTeam,setFilterTeam]=useState("ALL");
  const [search,setSearch]=useState("");
  const teams=players?[...new Set(players.map(p=>p.team))].sort():[];
  const displayed=players?players.filter(p=>(filterTeam==="ALL"||p.team===filterTeam)&&(!search||p.name.toLowerCase().includes(search.toLowerCase()))):[];

  function setRole(name,team,role){setPlayers(prev=>prev.map(p=>p.name===name&&p.team===team?{...p,lineRole:role}:p));}
  function bulkSet(role){if(!filterTeam||filterTeam==="ALL")return;setPlayers(prev=>prev.map(p=>p.team===filterTeam?{...p,lineRole:role}:p));}

  if(!players) return <Card><div style={{color:"var(--color-text-secondary)",fontSize:12,padding:8}}>Load player data first</div></Card>;

  const ALL_ROLES=["TOP6","MID6","BOT6","D1","D2","D3","STARTER","BACKUP","SCRATCHED"];

  return <div>
    <Card style={{marginBottom:12}}>
      <div style={{display:"flex",gap:8,alignItems:"center",flexWrap:"wrap"}}>
        <input placeholder="Search…" value={search} onChange={e=>setSearch(e.target.value)}
          style={{padding:"5px 10px",fontSize:12,background:"var(--color-background-secondary)",border:"0.5px solid var(--color-border-secondary)",borderRadius:"var(--border-radius-md)",color:"var(--color-text-primary)",width:180}}/>
        <select value={filterTeam} onChange={e=>setFilterTeam(e.target.value)} style={SEL}>
          <option value="ALL">All Teams</option>
          {teams.map(t=><option key={t} value={t}>{t} – {TEAM_NAMES[t]}</option>)}
        </select>
        {filterTeam!=="ALL"&&<>
          <span style={{fontSize:10,color:"var(--color-text-secondary)"}}>Bulk (all positions):</span>
          {["SCRATCHED"].map(r=><button key={r} onClick={()=>bulkSet(r)} style={{padding:"3px 8px",fontSize:9,borderRadius:3,border:"none",cursor:"pointer",fontWeight:500,background:`${roleColor(r)}20`,color:roleColor(r)}}>→{r}</button>)}
        </>}
        <div style={{marginLeft:"auto",display:"flex",gap:4,flexWrap:"wrap"}}>
          {ALL_ROLES.map(r=>{const cnt=players.filter(p=>p.lineRole===r).length;if(!cnt)return null;const c=roleColor(r);return <div key={r} style={{fontSize:9,padding:"2px 7px",borderRadius:3,background:`${c}20`,color:c,fontWeight:500}}>{r}: {cnt}</div>;})}
        </div>
      </div>
      <div style={{marginTop:8,fontSize:10,color:"var(--color-text-tertiary)"}}>
        <strong style={{color:"#10b981"}}>F:</strong> TOP6 ×1.2, BOT6 ×0.75 &nbsp;|&nbsp;
        <strong style={{color:"#3b82f6"}}>D:</strong> D1 ×1.15, D2 ×1.0, D3 ×0.75 &nbsp;|&nbsp;
        <strong style={{color:"#a78bfa"}}>G:</strong> STARTER/BACKUP excluded from skater markets &nbsp;|&nbsp;
        SCRATCHED ×0
      </div>
    </Card>

    <Card>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <TH cols={["Player","Team","Pos","GP","G","A","PTS","SOG","HIT","BLK","Role","pGP","pG","pA","pSOG","pHIT","pBLK","pTK","pTSA","pGV"]}/>
          <tbody>{displayed.slice(0,300).map((p,i)=>{
            const roles=rolesForPos(p.pos);
            const curRole=p.lineRole||roles[0];
            return (
            <tr key={i} style={{borderBottom:"0.5px solid var(--color-border-tertiary)",
              background:p.lineRole==="SCRATCHED"?(dark?"rgba(239,68,68,0.06)":"rgba(239,68,68,0.04)"):i%2===0?"transparent":(dark?"rgba(255,255,255,0.018)":"rgba(0,0,0,0.012)")}}>
              <td style={{padding:"3px 8px",opacity:p.lineRole==="SCRATCHED"?0.45:1}}>{p.name}</td>
              <td style={{padding:"3px 8px",textAlign:"right"}}><span style={{fontSize:9,padding:"1px 4px",borderRadius:2,background:"rgba(59,130,246,0.12)",color:"#60a5fa"}}>{p.team}</span></td>
              <td style={{padding:"3px 8px",textAlign:"right",color:"var(--color-text-secondary)"}}>{p.pos}</td>
              {["gp","g","a","pts","sog","hit","blk"].map(f=><td key={f} style={{padding:"3px 8px",textAlign:"right",fontFamily:"var(--font-mono)",fontSize:10,color:"var(--color-text-secondary)"}}>{Math.round(p[f]||0)}</td>)}
              <td style={{padding:"3px 6px",textAlign:"right"}}>
                <select value={curRole} onChange={e=>setRole(p.name,p.team,e.target.value)}
                  style={{fontSize:9,padding:"2px 4px",background:`${roleColor(curRole)}18`,border:`0.5px solid ${roleColor(curRole)}`,borderRadius:3,color:roleColor(curRole),fontWeight:500}}>
                  {roles.map(r=><option key={r} value={r}>{r}</option>)}
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
// SETTINGS TAB
// ═══════════════════════════════════════════════════════════════════════════════
function SettingsTab({globals,setGlobals,margins,setMargins,showTrue,setShowTrue,showDec,setShowDec,dark}) {
  return (
    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:16,alignItems:"start"}}>
      <Card>
        <SH title="Global Controls"/>
        {[{k:"overroundR1",l:"R1 Leader Overround",min:1,max:1.5,step:0.01},{k:"overroundFull",l:"Full Playoff Overround",min:1,max:1.5,step:0.01},{k:"powerFactor",l:"Power Factor",min:0.5,max:2,step:0.05},{k:"rateDiscount",l:"Rate Discount",min:0.5,max:1,step:0.01},{k:"dispersion",l:"NB Dispersion (r)",min:1,max:5,step:0.1}].map(({k,l,min,max,step})=>(
          <div key={k} style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
            <label style={{fontSize:11,color:"var(--color-text-secondary)"}}>{l}</label>
            <NI value={globals[k]} onChange={v=>setGlobals(g=>({...g,[k]:v}))} min={min} max={max} step={step}/>
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
            <NI value={v} onChange={nv=>setMargins(m=>({...m,[k]:nv}))} min={1} max={1.5} step={0.01} style={{width:58}}/>
          </div>
        ))}
      </Card>

      <Card>
        <SH title="Supabase Sync"/>
        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10}}>
          <span style={{fontSize:11,color:"var(--color-text-secondary)"}}>Status:</span>
          <SyncBadge status={SUPABASE_ENABLED?"ok":"idle"}/>
        </div>
        <div style={{fontSize:10,color:"var(--color-text-tertiary)",lineHeight:1.9,padding:"10px 12px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)",marginBottom:10}}>
          <strong style={{color:"var(--color-text-secondary)",display:"block",marginBottom:6}}>One-time setup:</strong>
          <span style={{display:"block",marginBottom:4}}>1. Create free project at <strong>supabase.com</strong></span>
          <span style={{display:"block",marginBottom:4}}>2. Go to SQL Editor and run:</span>
          <pre style={{margin:"4px 0 8px",padding:"6px 8px",background:"var(--color-background-primary)",borderRadius:4,fontSize:9,overflowX:"auto",border:"0.5px solid var(--color-border-tertiary)"}}>
{`CREATE TABLE pricer_state (
  key TEXT PRIMARY KEY,
  value JSONB,
  updated_at TIMESTAMPTZ
);
ALTER TABLE pricer_state ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_rw" ON pricer_state
  FOR ALL TO anon USING (true) WITH CHECK (true);`}
          </pre>
          <span style={{display:"block",marginBottom:2}}>3. Copy <strong>Project URL</strong> + <strong>anon public key</strong> from Settings → API</span>
          <span style={{display:"block"}}>4. Set <code style={{fontSize:9}}>SUPABASE_URL</code> + <code style={{fontSize:9}}>SUPABASE_KEY</code> at top of the JSX file</span>
        </div>
        <div style={{padding:"8px 12px",background:"var(--color-background-secondary)",borderRadius:"var(--border-radius-md)"}}>
          <SH title="Model Reference"/>
          <div style={{fontSize:10,color:"var(--color-text-tertiary)",lineHeight:1.9}}>
            <strong style={{color:"var(--color-text-secondary)"}}>Rate discount:</strong> 1.0 pre-playoffs → 0.85 default → trends to 1.0 as actuals fill<br/>
            <strong style={{color:"var(--color-text-secondary)"}}>Power factor &gt;1:</strong> squeezes longshots, more overround at the tail<br/>
            <strong style={{color:"var(--color-text-secondary)"}}>Dispersion r:</strong> NB vs Poisson — higher r = fatter goal tail<br/>
            <strong style={{color:"var(--color-text-secondary)"}}>Sync:</strong> debounced 3s after each change, all state keys stored in Supabase
          </div>
        </div>
      </Card>
    </div>
  );
}
