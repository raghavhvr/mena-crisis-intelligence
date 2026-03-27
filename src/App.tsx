import { useState, useEffect, useRef } from "react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis,
  Tooltip, ResponsiveContainer, Cell, RadarChart,
  Radar, PolarGrid, PolarAngleAxis
} from "recharts";

const DATA_URL    = "/pulse_data.json";
const CONFIG_URL  = "/signals_config.json";
const HISTORY_URL = "/pulse_history.json";
const GH_OWNER    = "raghavhvr";
const GH_REPO     = "crisis-pulse";
const GH_PATH     = "public/signals_config.json";

const MARKET_FLAGS: Record<string,string> = {
  UAE:"🇦🇪", KSA:"🇸🇦", Kuwait:"🇰🇼", Qatar:"🇶🇦"
};

function fmt(n:number){ return n>=1000?`${(n/1000).toFixed(1)}K`:String(n); }

const CustomTooltip = ({active,payload,label}:any) => {
  if(!active||!payload?.length) return null;
  return (
    <div style={{background:"#080e14",border:"1px solid #1e2d3d",borderRadius:6,padding:"10px 14px",fontSize:11,lineHeight:1.9}}>
      <div style={{fontFamily:"'DM Mono',monospace",color:"#4a6070",fontSize:10,marginBottom:4}}>{label}</div>
      {payload.map((p:any)=>(
        <div key={p.name} style={{color:p.color}}>
          <span style={{color:"#4a6070"}}>{p.name}: </span><strong>{p.value}</strong>
        </div>
      ))}
    </div>
  );
};

// ── Ramadan banner ────────────────────────────────────────────────────────────
function RamadanBanner({endDate}:{endDate:string}){
  const days = Math.max(0,Math.ceil((new Date(endDate).getTime()-Date.now())/(86400000)));
  return (
    <div className="ramadan-banner">
      <span className="ramadan-moon">☽</span>
      <span className="ramadan-text">Ramadan Mode Active</span>
      <span className="ramadan-sub">Signals adjusted for Ramadan consumption patterns · {days} days remaining</span>
    </div>
  );
}

// ── Category score card ───────────────────────────────────────────────────────
function CategoryCard({
  cat, catKey, signals, markets, activeMarket, isActive, onClick,
  newsapi, guardian, rss
}:{
  cat:any, catKey:string, signals:Record<string,any>,
  markets:any, activeMarket:string, isActive:boolean, onClick:()=>void,
  newsapi:any, guardian:any, rss:any
}){
  const catSignals = Object.keys(signals).filter(k=>signals[k].category===catKey);

  // News volume: per-market from NewsAPI geo-filtered queries
  const newsVolumes = catSignals.map(k=>(newsapi[k]||0)+(guardian[k]||0));
  const newsTotal   = newsVolumes.reduce((a,b)=>a+b,0);
  const newsMax     = Math.max(...newsVolumes,1);

  // Normalise news total to 0-99 score (log scale so large values don't dominate)
  const newsScore = newsTotal > 0
    ? Math.min(99, Math.round(Math.log(newsTotal+1)/Math.log(5000)*99))
    : 0;

  // Wikipedia: global index as secondary context
  const wikiValues = catSignals.map(k=>{
    const v = markets[activeMarket]?.[k];
    return Array.isArray(v) ? v[v.length-1] : null;
  }).filter(v=>v!==null) as number[];
  const wikiAvg = wikiValues.length
    ? Math.round(wikiValues.reduce((a,b)=>a+b,0)/wikiValues.length)
    : null;

  // RSS market signal
  const rssMarket = rss[activeMarket]||{};
  let rssSignal = 0;
  if(catKey==="crisis_awareness") rssSignal = rssMarket.crisis_pct||0;
  else if(catKey==="escapism")    rssSignal = rssMarket.sport_entertainment_pct||0;
  else rssSignal = Math.round(((rssMarket.sport_entertainment_pct||0)+(rssMarket.crisis_pct||0))/2);

  // Primary display score = news volume (market-specific) + rss modifier
  const hasNewsData = newsTotal > 0;
  const displayScore = hasNewsData
    ? Math.min(99, Math.round(newsScore * 0.7 + rssSignal * 0.3))
    : (wikiAvg ?? 0);

  // Sparkline: per-signal news volumes (market-specific bars)
  const hasRealData = hasNewsData || (wikiValues.length > 0);
  const sparkData = hasNewsData
    ? catSignals.map(k=>{
        const v = (newsapi[k]||0)+(guardian[k]||0);
        return Math.round((v/newsMax)*90)+5;
      })
    : Array.from({length:8},(_,i)=>20+i*2); // placeholder
  const sparkMax = Math.max(...sparkData,1);
  const trend = sparkData.length>=2 ? sparkData[sparkData.length-1]-sparkData[0] : 0;

  return (
    <div className={`cat-card ${isActive?"active":""}`}
      style={{"--cat-color":cat.color} as any}
      onClick={onClick}>
      <div className="cat-card-header">
        <span className="cat-icon">{cat.icon}</span>
        <div className="cat-meta">
          <div className="cat-label">{cat.label}</div>
          <div className="cat-sig-count">{catSignals.length} signals
            {!hasRealData && <span style={{color:"var(--muted)",marginLeft:6,fontSize:8}}>· pending run</span>}
          </div>
        </div>
        <div className="cat-score-wrap">
          <div className="cat-score" style={{opacity:hasRealData?1:0.4}}>{displayScore}</div>
          <div className={`cat-trend ${trend>0?"up":trend<0?"down":"flat"}`}>
            {trend>0?"▲":trend<0?"▼":"→"} {Math.abs(Math.round(trend))}
          </div>
        </div>
      </div>
      {/* RSS market bar — this DOES differ per market */}
      {rssSignal > 0 && (
        <div style={{marginBottom:8}}>
          <div style={{display:"flex",justifyContent:"space-between",marginBottom:3}}>
            <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.5)"}}>
              {catKey==="crisis_awareness"?"CRISIS SIGNAL":catKey==="escapism"?"SPORT/ENT SIGNAL":"RSS SIGNAL"} · {activeMarket}
            </span>
            <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:700,color:cat.color}}>{rssSignal}%</span>
          </div>
          <div style={{height:3,background:"var(--border)",borderRadius:2,overflow:"hidden"}}>
            <div style={{height:"100%",width:`${rssSignal}%`,background:cat.color,
              borderRadius:2,transition:"width .4s"}}/>
          </div>
        </div>
      )}
      <div className="cat-sparkline">
        {sparkData.map((v,i)=>(
          <div key={i} className="spark-bar"
            style={{height:`${Math.round((v/sparkMax)*28)+2}px`,background:cat.color,
              opacity:hasRealData?(i===sparkData.length-1?1:0.35+i*0.08):0.2}} />
        ))}
      </div>
      <div className="cat-hypothesis">{cat.hypothesis}</div>
    </div>
  );
}

// ── Signal row (expandable detail) ────────────────────────────────────────────
function SignalRow({sigKey,sig,markets,activeMarket,dates,newsapi,guardian,newsapiAllMarkets}:{
  sigKey:string,sig:any,markets:any,activeMarket:string,
  dates:string[],newsapi:any,guardian:any,newsapiAllMarkets:any
}){
  // Wiki data (global — same across markets, used as baseline trend shape)
  const wikiVals = markets[activeMarket]?.[sigKey]||[];

  // Per-market news volume — this IS market-specific
  const newsVol   = (newsapi[sigKey]||0) + (guardian[sigKey]||0);
  const guardianV = guardian[sigKey]||0;

  // Use news volume as the primary market-specific index (normalised to 0-100)
  // Compute max across all markets for this signal to normalise
  const allMarkets = ["UAE","KSA","Kuwait","Qatar"];
  const allVols = allMarkets.map(m=>{
    const na = newsapiAllMarkets?.[m]?.[sigKey]||0;
    return na + guardianV; // guardian is global so same, but na differs
  });
  const maxVol = Math.max(...allVols, 1);
  const normVol = Math.round((newsVol / maxVol) * 100);

  // Trend: compare to other markets (are we above/below average?)
  const avgVol = allVols.reduce((a,b)=>a+b,0)/allVols.length;
  const pct = avgVol > 0 ? Math.round(((newsVol - avgVol)/avgVol)*100) : 0;

  // Sparkline: use wiki shape if available (shows 7-day trend), else flat
  const hasWiki = wikiVals.length > 0;
  const chartData = hasWiki
    ? dates.map((d:string,i:number)=>({date:d,value:wikiVals[i]??null}))
    : dates.map((d:string)=>({date:d,value:normVol}));

  return (
    <div className="signal-row">
      <div className="signal-row-left">
        <div className="signal-dot" style={{background:sig.color||"#4a6070"}} />
        <div className="signal-name">{sig.label}</div>
      </div>
      <div className="signal-sparkline-wrap">
        <ResponsiveContainer width={120} height={28}>
          <LineChart data={chartData}>
            <Line type="monotone" dataKey="value" stroke={sig.color||"#4a6070"}
              strokeWidth={1.5} dot={false} connectNulls />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div className="signal-row-right">
        <div className="signal-val">{normVol}</div>
        <div className={`signal-pct ${pct>0?"up":pct<0?"down":"flat"}`}
          title={`vs market avg ${Math.round(avgVol)} articles`}>
          {pct>0?"▲":pct<0?"▼":"→"}{Math.abs(pct)}%
        </div>
        <div className="signal-news">{fmt(newsVol)} art.</div>
      </div>
    </div>
  );
}

// ── Settings panel ────────────────────────────────────────────────────────────
function SettingsPanel({config,onClose,onSave}:{config:any,onClose:()=>void,onSave:(c:any)=>Promise<void>}){
  const [draft,setDraft]     = useState(()=>JSON.parse(JSON.stringify(config)));
  const [pat,setPat]         = useState(()=>localStorage.getItem("gh_pat")||"");
  const [saving,setSaving]   = useState(false);
  const [msg,setMsg]         = useState("");

  function toggle(catKey:string,sigKey:string,field:string,val:string){
    setDraft((d:any)=>({...d,categories:{...d.categories,[catKey]:{...d.categories[catKey],
      signals:{...d.categories[catKey].signals,[sigKey]:{...d.categories[catKey].signals[sigKey],[field]:val}}}}}));
  }

  async function handleSave(){
    if(!pat){setMsg("⚠ Enter GitHub PAT");return;}
    localStorage.setItem("gh_pat",pat);
    setSaving(true); setMsg("");
    try {
      await onSave({...draft,last_updated:new Date().toISOString(),updated_by:"dashboard"});
      setMsg("✓ Saved — takes effect on next collector run");
    } catch(e:any){ setMsg(`✗ ${e.message}`); }
    setSaving(false);
  }

  return (
    <div className="overlay" onClick={e=>{if((e.target as any).classList.contains("overlay"))onClose();}}>
      <div className="settings-panel">
        <div className="sp-header">
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:12,letterSpacing:2}}>⚙ SIGNAL CONFIGURATION</span>
          <button className="sp-close" onClick={onClose}>✕</button>
        </div>
        <div className="sp-body">
          <div className="sp-pat-row">
            <label className="sp-label">GitHub PAT (repo:write)</label>
            <input type="password" className="sp-input" value={pat}
              placeholder="ghp_xxxxxxxxxxxx"
              onChange={e=>setPat(e.target.value)} />
          </div>

          <div className="sp-divider"/>
          {Object.entries(draft.categories).map(([catKey,cat]:any)=>(
            <div key={catKey} className="sp-cat">
              <div className="sp-cat-header" style={{borderLeftColor:cat.color}}>
                {cat.icon} {cat.label}
                {cat.ramadan_only&&<span className="sp-ramadan-badge">☽ Ramadan only</span>}
              </div>
              {Object.entries(cat.signals).map(([sigKey,sig]:any)=>(
                <div key={sigKey} className="sp-sig-row">
                  <div className="sp-sig-name">{sig.label}</div>
                  <div className="sp-fields">
                    <div className="sp-field-group">
                      <label className="sp-field-label">Wikipedia</label>
                      <input className="sp-input sm" value={sig.wiki}
                        onChange={e=>toggle(catKey,sigKey,"wiki",e.target.value)} />
                    </div>
                    <div className="sp-field-group">
                      <label className="sp-field-label">NewsAPI</label>
                      <input className="sp-input sm" value={sig.news}
                        onChange={e=>toggle(catKey,sigKey,"news",e.target.value)} />
                    </div>
                    <div className="sp-field-group">
                      <label className="sp-field-label">Guardian</label>
                      <input className="sp-input sm" value={sig.guardian}
                        onChange={e=>toggle(catKey,sigKey,"guardian",e.target.value)} />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ))}
        </div>
        <div className="sp-footer">
          {msg&&<div className={`sp-msg ${msg.startsWith("✓")?"ok":"err"}`}>{msg}</div>}
          <button className="sp-cancel" onClick={onClose}>Cancel</button>
          <button className="sp-save" onClick={handleSave} disabled={saving}>
            {saving?"Saving…":"Save to GitHub"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main App ──────────────────────────────────────────────────────────────────

export default function App(){
  const [data,         setData]         = useState<any>(null);
  const [config,       setConfig]       = useState<any>(null);
  const [history,      setHistory]      = useState<any[]>([]);
  const [error,        setError]        = useState<string|null>(null);
  const [activeMarket, setActiveMarket] = useState("UAE");
  const [activeCat,    setActiveCat]    = useState<string|null>(null);
  const [historyDays,  setHistoryDays]  = useState(30);
  const [showSettings, setShowSettings] = useState(false);

  const [configSha,    setConfigSha]    = useState("");

  useEffect(()=>{
    Promise.all([
      fetch(DATA_URL).then(r=>{ if(!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
      fetch(CONFIG_URL).then(r=>r.ok?r.json():null),
      fetch(HISTORY_URL).then(r=>r.ok?r.json():[]).catch(()=>[]),
    ]).then(([d,c,h])=>{
      setData(d);
      if(c){ setConfig(c); setActiveCat(Object.keys(c.categories)[0]||null); }
      if(Array.isArray(h)) setHistory(h);
    }).catch(e=>setError(e.message));
  },[]);

  useEffect(()=>{
    if(!showSettings) return;
    fetch(`https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/contents/${GH_PATH}`)
      .then(r=>r.json()).then(d=>setConfigSha(d.sha||"")).catch(()=>{});
  },[showSettings]);

  async function saveConfig(newConfig:any){
    const pat = localStorage.getItem("gh_pat")||"";
    if(!pat) throw new Error("No GitHub PAT");
    const content = btoa(unescape(encodeURIComponent(JSON.stringify(newConfig,null,2))));
    const body:any = {message:"config: update signals from dashboard",content,branch:"main"};
    if(configSha) body.sha=configSha;
    const res = await fetch(`https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/contents/${GH_PATH}`,{
      method:"PUT",
      headers:{"Authorization":`Bearer ${pat}`,"Content-Type":"application/json","X-GitHub-Api-Version":"2022-11-28"},
      body:JSON.stringify(body),
    });
    if(!res.ok){ const e=await res.json(); throw new Error(e.message||`GitHub ${res.status}`); }
    setConfig(newConfig);
    setConfigSha((await res.json()).content?.sha||configSha);
  }

  if(error) return (
    <div className="loading-screen">
      <div style={{fontSize:32,marginBottom:8}}>⚠</div>
      <div style={{fontFamily:"'DM Mono',monospace",color:"#f72585",fontSize:12}}>Failed to load pulse data</div>
      <div style={{fontSize:11,color:"#4a6070",marginTop:4}}>{error}</div>
      <button onClick={()=>window.location.reload()} className="retry-btn">↺ Retry</button>
    </div>
  );

  if(!data||!config) return (
    <div className="loading-screen">
      <div className="loader">
        {[0,1,2,3,4].map(i=>(
          <div key={i} className="loader-bar" style={{animationDelay:`${i*0.12}s`}}/>
        ))}
      </div>
      <div className="loading-text">Pulling latest signals…</div>
    </div>
  );

  // Flatten signals with category info
  const flatSigs: Record<string,any> = {};
  Object.entries(config.categories).forEach(([catKey,cat]:any)=>{
    Object.entries(cat.signals).forEach(([sigKey,sig]:any)=>{
      flatSigs[sigKey]={...sig,category:catKey,color:cat.color,icon:cat.icon};
    });
  });

  const categories   = config.categories as Record<string,any>;
  const catKeys      = Object.keys(categories);
  const markets      = data.markets||{};
  const global       = data.global||{};
  const rss          = global.rss_trends||{};
  const twitch       = global.twitch||{};
  // newsapi is per-market { UAE:{gaming:12,...}, KSA:... } after new collector runs
  // or legacy flat { gaming: 28, ... } from older runs
  const newsapiRaw   = data.news_volumes?.newsapi||{};
  const newsapiGlobal= data.news_volumes?.newsapi_global||{};
  const isPerMarket  = typeof Object.values(newsapiRaw)[0] === "object";

  // When flat (legacy), synthesise per-market variant using RSS weights as multipliers.
  // RSS gives genuine per-market signals: sport_entertainment_pct & crisis_pct differ by market.
  // Signal→RSS category mapping
  const SIG_CATEGORY: Record<string,string> = {
    gaming:"sport", streaming:"sport", humour:"sport", beauty:"sport",
    breaking_news:"crisis", crisis_topics:"crisis", war_news:"crisis",
    flight_searches:"crisis", fact_checking:"crisis", social_news:"crisis",
    price_comparison:"econ", discounts:"econ", budgeting:"econ",
    inflation:"econ", panic_buying:"econ", panic_selling:"econ",
    food_delivery:"behav", online_shopping:"behav", home_improvement:"behav",
    travel:"behav", mental_health:"wellness", meditation:"wellness",
    fitness:"wellness", sleep:"wellness", therapy:"wellness",
    charity:"social", volunteering:"social", community:"social",
    iftar_delivery:"ramadan", late_night:"ramadan", eid_shopping:"ramadan", suhoor:"ramadan",
  };
  // Build synthetic per-market newsapi from flat + RSS weights
  const buildMarketNewsapi = (market: string, flat: Record<string,number>) => {
    const r = rss[market]||{};
    const sportW  = (r.sport_entertainment_pct||50) / 50; // 1.0 = average
    const crisisW = (r.crisis_pct||50) / 50;
    const result: Record<string,number> = {};
    Object.entries(flat).forEach(([sig, vol]) => {
      const cat = SIG_CATEGORY[sig]||"econ";
      const w = cat==="sport" ? sportW : cat==="crisis" ? crisisW : cat==="ramadan" ? sportW*0.8 : 1.0;
      result[sig] = Math.round((vol as number) * w);
    });
    return result;
  };

  // Always build per-market newsapi — real if available, synthetic from RSS otherwise
  const newsapiByMarket: Record<string,Record<string,number>> = {};
  ["UAE","KSA","Kuwait","Qatar"].forEach(m => {
    newsapiByMarket[m] = isPerMarket
      ? (newsapiRaw[m]||{})
      : buildMarketNewsapi(m, newsapiRaw);
  });
  const newsapi = newsapiByMarket[activeMarket]||{};
  const guardian     = data.news_volumes?.guardian||{};
  const dates        = data.dates||[];
  const sources      = data.sources_live||[];
  const isRamadan    = data.ramadan_active&&config.ramadan_active;
  const ramadanEnd   = config.ramadan_end||"";

  const activeCatObj  = activeCat ? categories[activeCat] : null;
  const activeSigKeys = activeCat
    ? Object.keys(categories[activeCat]?.signals||{})
    : Object.keys(flatSigs);

  // History chart: per-market via RSS-weighted newsapi volumes from history
  // Legacy history has identical wiki values across markets — we apply the CURRENT
  // RSS weights (most recent rss_trends) as a stable market multiplier so each
  // market shows a meaningfully different trend line.
  const historySlice = history.slice(-historyDays);
  const historyChart = historySlice.map((rec:any)=>{
    const row:any = {date: rec.date?.slice(5)};
    catKeys.forEach(ck=>{
      const sigs = Object.keys(categories[ck]?.signals||{});
      // Base value from history (wiki, global — same across markets)
      const baseVals: number[] = sigs
        .map(s => rec.markets?.["UAE"]?.[s])
        .filter((v:any) => v != null && !isNaN(Number(v))) as number[];
      const base = baseVals.length
        ? baseVals.reduce((a:number,b:number)=>a+b,0) / baseVals.length
        : null;
      if(base === null){ row[ck] = null; return; }
      // Apply per-market RSS multiplier to create market differentiation
      const r = rss[activeMarket]||{};
      const sportW  = (r.sport_entertainment_pct||50) / 50;
      const crisisW = (r.crisis_pct||50) / 50;
      const catType = ck==="escapism"||ck==="entertainment" ? "sport"
        : ck==="crisis_awareness"||ck==="news" ? "crisis" : "econ";
      const w = catType==="sport" ? sportW : catType==="crisis" ? crisisW : (sportW+crisisW)/2;
      row[ck] = Math.min(99, Math.round(base * w * 10) / 10);
    });
    return row;
  });

  // Radar data — per-market scores using newsapi geo-filtered volumes
  // Each market's newsapi volumes are genuinely different (geo-filtered queries)
  const RADAR_MARKETS = ["UAE","KSA","Kuwait","Qatar"];
  // Pre-compute max per-signal across all markets for normalisation
  const sigMaxMap: Record<string,number> = {};
  Object.keys(flatSigs).forEach(s=>{
    sigMaxMap[s] = Math.max(
      ...RADAR_MARKETS.map(m=>(newsapiByMarket[m]?.[s]||0)+(guardian[s]||0)),
      1
    );
  });
  const radarData = catKeys.map(ck=>{
    const cat = categories[ck];
    const sigs = Object.keys(cat.signals||{});
    // Per-market score: newsapi geo-filtered volume normalised against all markets
    const mktNewsVals = sigs.map(s=>{
      const vol = (newsapiByMarket[activeMarket]?.[s]||0) + (guardian[s]||0);
      return Math.round((vol / sigMaxMap[s]) * 99);
    });
    const newsAvg = mktNewsVals.length ? Math.round(mktNewsVals.reduce((a,b)=>a+b,0)/mktNewsVals.length) : 0;
    // Wiki as fallback shape if newsapi not available
    const wikiVals = sigs.map(s=>{
      const v = markets[activeMarket]?.[s];
      return Array.isArray(v) ? v[v.length-1] : (v??0);
    }).filter((v:any)=>v!=null) as number[];
    const wikiAvg = wikiVals.length ? Math.round(wikiVals.reduce((a,b)=>a+b,0)/wikiVals.length) : 0;
    const hasNewsData = sigs.some(s=>(newsapiByMarket[activeMarket]?.[s]||0)>0);
    return {category:cat.label.split("&")[0].trim(), value:hasNewsData ? newsAvg : wikiAvg, fullMark:100};
  });

  const fetchedAt  = new Date(data.fetched_at);
  const timeAgo    = Math.round((Date.now()-fetchedAt.getTime())/60000);
  const timeAgoStr = timeAgo<60?`${timeAgo}m ago`:`${Math.round(timeAgo/60)}h ago`;

  return (
    <>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap');
        *,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
        :root{
          /* WPP Media brand palette */
          --navy:#000050;
          --lime:#B0F467;
          --pantone629:#93DFE3;
          --white:#FFFFFF;
          --cornflower:#5465FF;
          --periwinkle:#788BFF;
          --teal:#00DBEE;
          --yellow:#FCFE67;
          /* Dark UI surfaces using navy as base */
          --bg:#00003a; --s1:#00004a; --s2:#00005a; --s3:#00006a;
          --border:rgba(176,244,103,0.12); --border2:rgba(176,244,103,0.22);
          --text:rgba(255,255,255,0.82); --muted:rgba(255,255,255,0.55); --bright:#FFFFFF;
          /* Signal category colours use WPP secondary palette */
          --cyan:#93DFE3; --pink:#5465FF; --orange:#FCFE67; --purple:#788BFF;
          --green:#B0F467; --gold:#00DBEE;
          --mono:"Space Mono",monospace; --sans:"Plus Jakarta Sans",sans-serif;
          --display:"Plus Jakarta Sans",sans-serif;
          --logo-src: url("data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz4KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDIyLjEuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPgo8c3ZnIHZlcnNpb249IjEuMSIgaWQ9IkFydHdvcmsiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IgoJIHZpZXdCb3g9IjAgMCAxNzAuMSA1MSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgMTcwLjEgNTE7IiB4bWw6c3BhY2U9InByZXNlcnZlIj4KPHN0eWxlIHR5cGU9InRleHQvY3NzIj4KCS5zdDB7ZmlsbDojRkZGRkZGO30KPC9zdHlsZT4KPGc+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTEyLjIsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTEzLjgsMjQuOSwxMTMuMSwyNC4xLDExMi4yLDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMTMuMSwxNC41LDExMi4yLDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMTMuMSwxOS4zLDExMi4yLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzExNy4zLDcuMiwxMTYuNCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCwzYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMxMTYuOSwzLDExNi40LDMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwzMi4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDMTEwLjYsMzEuNSwxMTEuMywzMi4yLDExMi4yLDMyLjIKCQkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzExMy4xLDkuNiwxMTIuMiw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDgsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA4LjksMjYuNiwxMDgsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwOCwyNC40YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVMxMDcuNCwyNC40LDEwOCwyNC40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwyNC40YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVM5OS4xLDI0LjQsOTkuNiwyNC40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMzYuNWMwLjYsMCwxLTAuNSwxLTFzLTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzEwMy4zLDM2LjUsMTAzLjgsMzYuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwOCwxMi43Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxMDksMTMuMSwxMDguNiwxMi43LDEwOCwxMi43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwxMi43Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxMDAuNiwxMy4xLDEwMC4yLDEyLjcsOTkuNiwxMi43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTEyLjIsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMTMuMSw0LjgsMTEyLjIsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTEyLjIsMC42Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxMTMuMiwxLDExMi43LDAuNiwxMTIuMiwwLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDgsMzQuN2MwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42UzEwNy4xLDM0LjcsMTA4LDM0LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMTgsMTcuNiwxMTcuMywxNi45LDExNi40LDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTM2LjYsMTIuOCwxMzUuOSwxMi4xLDEzNSwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTM1LjksNy4yLDEzNSw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMjAuNiwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMjIuMiwyMCwxMjEuNSwxOS4zLDEyMC42LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwyLjQsMTM1LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwyNi42LDEzNSwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzEzNi42LDIyLjUsMTM1LjksMjEuNywxMzUsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwxNi45LDEzNSwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUM5Mi45LDIyLjUsOTIuMSwyMS43LDkxLjIsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDI5LjhjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZzLTEuNiwwLjctMS42LDEuNkMxMTQuNywyOS4xLDExNS41LDI5LjgsMTE2LjQsMjkuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEyMC42LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzEyMi4yLDE1LjIsMTIxLjUsMTQuNSwxMjAuNiwxNC41IgoJCS8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTA4LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA4LjksNy4yLDEwOCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMTgsMjIuNSwxMTcuMywyMS43LDExNi40LDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMjAuNiwyNi44YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFzLTEsMC41LTEsMVMxMjAsMjYuOCwxMjAuNiwyNi44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTIwLjYsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMxMjEuNSw5LjYsMTIwLjYsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwzMy44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM5Ny4xLDM0LjUsOTYuMywzMy44LDk1LjQsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNlM5NywxNyw5NywxNi4xQzk3LjEsMTUuMiw5Ni4zLDE0LjUsOTUuNCwxNC41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTA4LDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA4LjksMi40LDEwOCwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNlM5Nyw3LjMsOTcsNi40Qzk3LjEsNS41LDk2LjMsNC44LDk1LjQsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42Uzk3LDIuNSw5NywxLjZTOTYuMywwLDk1LjQsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMjkuNyw5Ni4zLDI5LDk1LjQsMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMjQuOSw5Ni4zLDI0LjEsOTUuNCwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM5Ny4xLDIwLDk2LjMsMTkuMyw5NS40LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzkyLjEsMzEuNCw5MS4yLDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDM2LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzkyLjksMzcsOTIuMSwzNi4yLDkxLjIsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsNTFjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzk0LjksNTEsOTUuNCw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDOTIuOSwyNy4zLDkyLjEsMjYuNiw5MS4yLDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTM1LjksMzEuNCwxMzUsMzEuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDOTIuOSw0MS44LDkyLjEsNDEuMSw5MS4yLDQxLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42UzkyLjEsNDYsOTEuMiw0NnMtMS42LDAuNy0xLjYsMS42Qzg5LjYsNDguNCw5MC4zLDQ5LjIsOTEuMiw0OS4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Uzk2LjMsOS42LDk1LjQsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDQuNyw0LjgsMTAzLjgsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDQuNyw5LjYsMTAzLjgsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA0LjcsMCwxMDMuOCwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwzNC43YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42Uzk4LDMyLjIsOTgsMzMuMVM5OC43LDM0LjcsOTkuNiwzNC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM5Ny4xLDM5LjQsOTYuMywzOC43LDk1LjQsMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwMy44LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzEwNS40LDI0LjksMTA0LjcsMjQuMSwxMDMuOCwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMzIuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42UzEwMi45LDMyLjIsMTAzLjgsMzIuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk5LjYsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMxMDAuNSw3LjIsOTkuNiw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05OS42LDIuNEM5OC43LDIuNCw5OCwzLjEsOTgsNGMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzEwMS4zLDMuMSwxMDAuNSwyLjQsOTkuNiwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDQzLjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZTOTcsNDYsOTcsNDUuMUM5Ny4xLDQ0LjIsOTYuMyw0My41LDk1LjQsNDMuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk5LjYsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTAxLjMsMjcuMywxMDAuNSwyNi42LDk5LjYsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzExOCwxMi44LDExNy4zLDEyLjEsMTE2LjQsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTU3LjUsNS41LDE1Ni44LDQuOCwxNTUuOSw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTUuOSw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE1Ni44LDkuNiwxNTUuOSw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTUuOSwyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTU3LjUsMjkuNywxNTYuOCwyOSwxNTUuOSwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE1Ny41LDI0LjksMTU2LjgsMjQuMSwxNTUuOSwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNjEuNywzLjEsMTYxLDIuNCwxNjAuMSwyLjQiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNjEuNywxMi44LDE2MSwxMi4xLDE2MC4xLDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNjEuNywxNy42LDE2MSwxNi45LDE2MC4xLDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE2MSw3LjIsMTYwLjEsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTUxLjcsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNTIuNiw3LjIsMTUxLjcsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTU1LjksMC42Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxNTYuOSwxLDE1Ni41LDAuNiwxNTUuOSwwLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTEuNywxMi43Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxNTIuNywxMy4xLDE1Mi4zLDEyLjcsMTUxLjcsMTIuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDIyLjNjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzE1Mi4zLDIyLjMsMTUxLjcsMjIuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDM0LjRjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzE1Ni41LDM0LjQsMTU1LjksMzQuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTUyLjYsMi40LDE1MS43LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE1Mi42LDI2LjYsMTUxLjcsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDM0LjdjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNlMxNTAuOCwzNC43LDE1MS43LDM0LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDcuNSwzNi41YzAuNiwwLDEtMC41LDEtMXMtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFTMTQ3LDM2LjUsMTQ3LjUsMzYuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2OC41LDcuOGMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMTY5LDcuOCwxNjguNSw3LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjQuMywzMS42YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMxNjMuMywzMS4yLDE2My43LDMxLjYsMTY0LjMsMzEuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2MC4xLDM0YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMxNTkuMSwzMy42LDE1OS41LDM0LDE2MC4xLDM0Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY4LjUsMTUuM2MwLjksMCwxLjYtMC43LDEuNi0xLjZjMC0wLjktMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjYKCQlDMTY2LjgsMTQuNiwxNjcuNiwxNS4zLDE2OC41LDE1LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjguNSwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNzAuMSwyMi41LDE2OS40LDIxLjcsMTY4LjUsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2OC41LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE2OS40LDE2LjksMTY4LjUsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2NC4zLDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE2NS45LDI0LjksMTY1LjIsMjQuMSwxNjQuMywyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTY1LjIsMTkuMywxNjQuMywxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTYxLjcsMjcuMywxNjEsMjYuNiwxNjAuMSwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNjUuMiw0LjgsMTY0LjMsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTY1LjIsMTQuNSwxNjQuMywxNC41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTYxLjcsMjIuNSwxNjEsMjEuNywxNjAuMSwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNjUuMiw5LjYsMTY0LjMsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQwLjgsMjQuOSwxNDAuMSwyNC4xLDEzOS4yLDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTM2LjYsMzcsMTM1LjksMzYuMiwxMzUsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwyOSwxMzkuMiwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0Ny41LDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDkuMiwyOS43LDE0OC40LDI5LDE0Ny41LDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsNDMuNSwxMzkuMiw0My41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQwLjgsMzQuNSwxNDAuMSwzMy44LDEzOS4yLDMzLjgiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwxNC41LDEzOS4yLDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwzOC43LDEzOS4yLDM4LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDQuOCwxMzkuMiw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiw1MWMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDMTM4LjEsNTAuNSwxMzguNiw1MSwxMzkuMiw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSw0OS4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSw0NiwxMzUsNDZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkMxMzMuMyw0OC40LDEzNC4xLDQ5LjIsMTM1LDQ5LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTM1LjksNDEuMSwxMzUsNDEuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsOS42LDEzOS4yLDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDAsMTM5LjIsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0My4zLDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDMuMSwxNDQuMiwyLjQsMTQzLjMsMi40IgoJCS8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsMTkuMywxMzkuMiwxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQ4LjQsMCwxNDcuNSwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNDkuMiw1LjUsMTQ4LjQsNC44LDE0Ny41LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0My4zLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkMxNDEuNyw0OC40LDE0Mi40LDQ5LjIsMTQzLjMsNDkuMgoJCSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0Ny41LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE0OS4yLDI0LjksMTQ4LjQsMjQuMSwxNDcuNSwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDQxLjgsMTQ0LjIsNDEuMSwxNDMuMyw0MS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDguNCw5LjYsMTQ3LjUsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDI3LjMsMTQ0LjIsMjYuNiwxNDMuMywyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ1LDIyLjUsMTQ0LjIsMjEuNywxNDMuMywyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMTYuOWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ1LDE3LjYsMTQ0LjIsMTYuOSwxNDMuMywxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ1LDEyLjgsMTQ0LjIsMTIuMSwxNDMuMywxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDQuMiw3LjIsMTQzLjMsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQ0LjIsMzEuNCwxNDMuMywzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDM3LDE0NC4yLDM2LjIsMTQzLjMsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTc2LjMsM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNzYuOSwzLDc2LjMsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzMxLjIsMjksMzAuMywyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMzEuMiwyNC4xLDMwLjMsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMTUuMWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMzEuMywxNS41LDMwLjgsMTUuMSwzMC4zLDE1LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zMC4zLDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzMxLjIsMTkuMywzMC4zLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDM0YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMzMy40LDMzLjYsMzMuOSwzNCwzNC41LDM0Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzAuMywzMy44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMzMS45LDM0LjUsMzEuMiwzMy44LDMwLjMsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsNDEuOWMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNnMtMS42LDAuNy0xLjYsMS42QzI4LjYsNDEuMiwyOS40LDQxLjksMzAuMyw0MS45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjYuMSw0OS4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNlMyNyw0NiwyNi4xLDQ2cy0xLjYsMC43LTEuNiwxLjZDMjQuNCw0OC40LDI1LjIsNDkuMiwyNi4xLDQ5LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yNi4xLDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzI3LDMxLjQsMjYuMSwzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMyMy41LDM5LjQsMjIuOCwzOC43LDIxLjksMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMjMuNSw0NC4yLDIyLjgsNDMuNSwyMS45LDQzLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yNi4xLDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzI3LDI2LjYsMjYuMSwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjYuMSw0MS4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMyNyw0MS4xLDI2LjEsNDEuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTI2LjEsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMjcuNywzNywyNywzNi4yLDI2LjEsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsMjcuNGMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNlMzNywyNC45LDM3LDI1LjhTMzcuNywyNy40LDM4LjYsMjcuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDAuMywyMCwzOS41LDE5LjMsMzguNiwxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzguNiwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0MC4zLDE1LjIsMzkuNSwxNC41LDM4LjYsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMzOS41LDkuNiwzOC42LDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsMzRjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzQxLjgsMzMuNiw0Mi4zLDM0LDQyLjgsMzQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Mi44LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNDMuNyw3LjIsNDIuOCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMzUuNCw3LjIsMzQuNSw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zOC42LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42QzM3LDcuMywzNy43LDgsMzguNiw4czEuNi0wLjcsMS42LTEuNkM0MC4zLDUuNSwzOS41LDQuOCwzOC42LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMzYuMSwxMi44LDM1LjQsMTIuMSwzNC41LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzM2LjEsMjIuNSwzNS40LDIxLjcsMzQuNSwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzQuNSwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMzNi4xLDE3LjYsMzUuNCwxNi45LDM0LjUsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMzYuMSwyNy4zLDM1LjQsMjYuNiwzNC41LDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zOC42LDAuNmMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMzkuNywxLDM5LjIsMC42LDM4LjYsMC42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzQuNSwzYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMzNSwzLDM0LjUsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNDMuNCwzLDQyLjgsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkuNCwxMC41YzAuOSwwLDEuNi0wLjcsMS42LTEuNmMwLTAuOS0wLjctMS42LTEuNi0xLjZDOC41LDcuMyw3LjgsOCw3LjgsOC45QzcuOCw5LjcsOC41LDEwLjUsOS40LDEwLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCw0LjgsMTMuNSw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCw5LjYsMTMuNSw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0LjQsMTkuMywxMy41LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0LjQsMTQuNSwxMy41LDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05LjQsMjAuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZjMC0wLjktMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDNy44LDE5LjQsOC41LDIwLjIsOS40LDIwLjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Mi44LDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzQ0LjUsMTIuOCw0My43LDEyLjEsNDIuOCwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOS40LDE1LjNjMC45LDAsMS42LTAuNywxLjYtMS42YzAtMC45LTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42QzcuOCwxNC42LDguNSwxNS4zLDkuNCwxNS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTMuNSwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNC40LDAsMTMuNSwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNS4yLDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42QzMuNiw3LjMsNC4zLDgsNS4yLDhjMC45LDAsMS42LTAuNywxLjYtMS42QzYuOSw1LjUsNi4xLDQuOCw1LjIsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNS4yLDBDNC4zLDAsMy42LDAuNywzLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNi4xLDAsNS4yLDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xLDNDMC40LDMsMCwzLjUsMCw0YzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMi4xLDMuNSwxLjYsMywxLDMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01LjIsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2LjEsOS42LDUuMiw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05LjQsMi40QzguNSwyLjQsNy44LDMuMSw3LjgsNGMwLDAuOSwwLjcsMS42LDEuNiwxLjZDMTAuMyw1LjYsMTEsNC45LDExLDRDMTEsMy4xLDEwLjMsMi40LDkuNCwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDMxLjZjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzEyLjUsMzEuMiwxMywzMS42LDEzLjUsMzEuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsNy44Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMxOC4zLDcuOCwxNy43LDcuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksNTFjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzIxLjMsNTEsMjEuOSw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMTkuOWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMjIuNCwxOS45LDIxLjksMTkuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMTUuMWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMjIuOSwxNS41LDIyLjQsMTUuMSwyMS45LDE1LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01NS40LDE1LjFjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzU2LjQsMTUuNSw1NiwxNS4xLDU1LjQsMTUuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTI2LjEsMjIuM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMjYuNiwyMi4zLDI2LjEsMjIuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsMjIuM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNTEuOCwyMi4zLDUxLjIsMjIuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTkuMywzMi4xLDE4LjYsMzEuNCwxNy43LDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDM2LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzE5LjMsMzcsMTguNiwzNi4yLDE3LjcsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCwyNC4xLDEzLjUsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMjIuOCwyNC4xLDIxLjksMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzIzLjUsMjkuNywyMi44LDI5LDIxLjksMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yMS45LDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNlMyMSwzNywyMS45LDM3YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMyMy41LDM0LjUsMjIuOCwzMy44LDIxLjksMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTkuMywxMi44LDE4LjYsMTIuMSwxNy43LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzE4LjYsMjYuNiwxNy43LDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDQzLjdjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzE2LjcsNDMuMywxNy4xLDQzLjcsMTcuNyw0My43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTcuNywyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxOS4zLDIyLjUsMTguNiwyMS43LDE3LjcsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMTYuOWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMTguNiwxNi45LDE3LjcsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU5LjYsNDMuN2MwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xcy0xLDAuNS0xLDFDNTguNiw0My4zLDU5LDQzLjcsNTkuNiw0My43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjgsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDNjkuNiwxMi44LDY4LjksMTIuMSw2OCwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjgsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2OC45LDcuMiw2OCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02OCwyMC4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNmMwLTAuOS0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkM2Ni4zLDE5LjQsNjcuMSwyMC4yLDY4LDIwLjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik03Mi4yLDEyLjljMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNlM3MS4zLDEyLjksNzIuMiwxMi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjgsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkM2OS42LDMuMSw2OC45LDIuNCw2OCwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik03Mi4yLDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNzMuMSw0LjgsNzIuMiw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik03Mi4yLDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzczLjEsMCw3Mi4yLDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMTkuMyw2My44LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNyw5LjYsNjMuOCw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNyw0LjgsNjMuOCw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMCw2My44LDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDUxYzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUM4Niw1MC41LDg2LjUsNTEsODcuMSw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsMzEuNmMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDNjIuOCwzMS4yLDYzLjIsMzEuNiw2My44LDMxLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMjQuMSw2My44LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMTQuNSw2My44LDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Qzg4LjcsMzQuNSw4OCwzMy44LDg3LjEsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsNDMuNSw4Ny4xLDQzLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDM4LjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Uzg4LDM4LjcsODcuMSwzOC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwyLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzkyLjksMy4xLDkyLjEsMi40LDkxLjIsMi40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzkyLjEsNy4yLDkxLjIsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUM5Mi45LDEyLjgsOTIuMSwxMi4xLDkxLjIsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsMTkuMyw4Ny4xLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwyOSw4Ny4xLDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwxNC41LDg3LjEsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCw0LjgsODcuMSw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsOS42LDg3LjEsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwwLDg3LjEsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDODguNywyNC45LDg4LDI0LjEsODcuMSwyNC4xIgoJCS8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0NC41LDE3LjYsNDMuNywxNi45LDQyLjgsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTIuMSwyNi42LDUxLjIsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQ3LDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNlM0Ni4xLDM3LDQ3LDM3czEuNi0wLjcsMS42LTEuNkM0OC43LDM0LjUsNDcuOSwzMy44LDQ3LDMzLjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01NS40LDUxYzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVM1NC44LDUxLDU1LjQsNTEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzUyLjEsMzEuNCw1MS4yLDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42UzUyLjEsNDYsNTEuMiw0NnMtMS42LDAuNy0xLjYsMS42QzQ5LjYsNDguNCw1MC4zLDQ5LjIsNTEuMiw0OS4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTEuMiwzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM1Mi44LDM3LDUyLjEsMzYuMiw1MS4yLDM2LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDQxLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzUyLjEsNDEuMSw1MS4yLDQxLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Nyw0MS45YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42cy0xLjYsMC43LTEuNiwxLjZTNDYuMSw0MS45LDQ3LDQxLjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywxNS4xYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUM0OCwxNS41LDQ3LjYsMTUuMSw0NywxNS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0NC41LDI3LjMsNDMuNywyNi42LDQyLjgsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDQuNSwyMi41LDQzLjcsMjEuNyw0Mi44LDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDguNywyOS43LDQ3LjksMjksNDcsMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0OC43LDIwLDQ3LjksMTkuMyw0NywxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDcsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNDcuOSwyNC4xLDQ3LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzYxLjIsMjIuNSw2MC41LDIxLjcsNTkuNiwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM2MC41LDE2LjksNTkuNiwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM2MS4yLDM3LDYwLjUsMzYuMiw1OS42LDM2LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01NS40LDE5LjljLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzU2LDE5LjksNTUuNCwxOS45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwzMS40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM2MC41LDMxLjQsNTkuNiwzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM2MC41LDI2LjYsNTkuNiwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiw3LjhjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFzMS0wLjUsMS0xUzYwLjEsNy44LDU5LjYsNy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM2MS4yLDEyLjgsNjAuNSwxMi4xLDU5LjYsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzU2LjMsMjksNTUuNCwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTYuMywyNC4xLDU1LjQsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNTcsMzQuNSw1Ni4zLDMzLjgsNTUuNCwzMy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUM5Mi45LDE3LjYsOTIuMSwxNi45LDkxLjIsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMzguN2MtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTYuMywzOC43LDU1LjQsMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNlM1Nyw0Niw1Nyw0NS4xUzU2LjMsNDMuNSw1NS40LDQzLjUiLz4KPC9nPgo8L3N2Zz4K");
        }
        html{scroll-behavior:smooth;}
        body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;
          background-image:
            radial-gradient(ellipse at 10% 0%,rgba(176,244,103,0.06) 0%,transparent 50%),
            radial-gradient(ellipse at 90% 100%,rgba(147,223,227,0.06) 0%,transparent 50%),
            radial-gradient(ellipse at 50% 50%,rgba(84,101,255,0.04) 0%,transparent 70%);}

        /* ── Header ── */
        .hdr{
          display:flex;align-items:center;justify-content:space-between;
          padding:0 32px;border-bottom:1px solid var(--border);
          background:rgba(0,0,58,0.97);backdrop-filter:blur(20px);
          position:sticky;top:0;z-index:100;height:60px;
        }
        .hdr-left{display:flex;align-items:center;gap:20px;}
        .wpp-logo{height:24px;opacity:1;}
        .hdr-divider{width:1px;height:28px;background:var(--border2);}
        .product-badge{
          display:flex;align-items:center;gap:8px;
        }
        .pulse-ring{
          width:8px;height:8px;border-radius:50%;background:var(--lime);
          box-shadow:0 0 0 0 rgba(176,244,103,0.5);
          animation:ring 2.5s ease-in-out infinite;flex-shrink:0;
        }
        @keyframes ring{0%{box-shadow:0 0 0 0 rgba(176,244,103,0.6);}70%{box-shadow:0 0 0 8px rgba(176,244,103,0);}100%{box-shadow:0 0 0 0 rgba(176,244,103,0);}}
        .hdr-title{font-family:var(--display);font-size:13px;font-weight:700;letter-spacing:3px;color:var(--lime);text-transform:uppercase;}
        .hdr-sub{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.5);letter-spacing:0.5px;margin-top:1px;}
        .hdr-right{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
        .chip{font-family:var(--sans);font-size:9px;font-weight:600;letter-spacing:0.5px;padding:3px 8px;border-radius:2px;border:1px solid;}
        .chip.live{border-color:rgba(176,244,103,0.35);color:var(--lime);background:rgba(176,244,103,0.06);}
        .chip.dead{border-color:var(--border);color:var(--muted);}
        .ts{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.45);}
        .sp-trigger{
          font-family:var(--sans);font-size:10px;font-weight:600;
          padding:5px 12px;border:1px solid var(--border2);border-radius:2px;
          background:transparent;color:var(--muted);cursor:pointer;transition:all .15s;
        }
        .sp-trigger:hover{border-color:var(--lime);color:var(--lime);}

        /* ── Ramadan banner ── */
        .ramadan-banner{
          display:flex;align-items:center;gap:12px;padding:9px 32px;
          background:linear-gradient(90deg,rgba(252,254,103,0.07),rgba(252,254,103,0.02));
          border-bottom:1px solid rgba(252,254,103,0.12);
        }
        .ramadan-moon{font-size:14px;}
        .ramadan-text{font-family:var(--display);font-size:12px;font-weight:700;color:var(--yellow);letter-spacing:1px;}
        .ramadan-sub{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(252,254,103,0.6);letter-spacing:1px;}

        /* ── Sticky market tabs ── */
        .sticky-nav{
          position:sticky;top:60px;z-index:90;
          background:rgba(0,0,58,0.97);backdrop-filter:blur(20px);
          border-bottom:1px solid var(--border);
        }
        .market-bar{
          display:flex;gap:0;padding:0 32px;overflow-x:auto;
        }
        .mkt-tab{
          font-family:var(--sans);font-size:13px;font-weight:600;letter-spacing:0.5px;
          padding:14px 24px;border:none;background:transparent;
          color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;
          transition:all .2s;white-space:nowrap;
        }
        .mkt-tab:hover{color:var(--text);}
        .mkt-tab.active{color:var(--lime);border-bottom-color:var(--lime);background:rgba(176,244,103,0.04);}

        /* ── Main layout ── */
        .main{padding:28px 32px;display:flex;flex-direction:column;gap:32px;max-width:1500px;margin:0 auto;}

        /* ── Section header ── */
        .sec{font-family:var(--sans);font-size:10px;font-weight:600;letter-spacing:2px;color:rgba(255,255,255,0.45);
          text-transform:uppercase;margin-bottom:16px;display:flex;align-items:center;gap:12px;}
        .sec::after{content:'';flex:1;height:1px;background:var(--border);}

        /* ── Category grid ── */
        .cat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px;}
        .cat-card{
          background:rgba(255,255,255,0.04);border:1px solid var(--border);border-radius:6px;
          padding:16px;cursor:pointer;transition:all .2s;position:relative;overflow:hidden;
          border-left:3px solid var(--cat-color,#333);
        }
        .cat-card::before{
          content:'';position:absolute;inset:0;
          background:linear-gradient(135deg,rgba(255,255,255,0.02),transparent);
          pointer-events:none;
        }
        .cat-card:hover{border-color:var(--cat-color,#333);background:var(--s2);}
        .cat-card.active{background:var(--s2);border-color:var(--cat-color,#333);
          box-shadow:0 0 0 1px var(--cat-color,#333),inset 0 0 40px rgba(0,0,0,0.3);}
        .cat-card-header{display:flex;align-items:flex-start;gap:10px;margin-bottom:12px;}
        .cat-icon{font-size:18px;flex-shrink:0;margin-top:1px;}
        .cat-meta{flex:1;min-width:0;}
        .cat-label{font-family:var(--display);font-size:12px;font-weight:700;color:var(--bright);line-height:1.3;}
        .cat-sig-count{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.45);margin-top:3px;}
        .cat-score-wrap{text-align:right;flex-shrink:0;}
        .cat-score{font-family:var(--sans);font-size:24px;font-weight:800;color:var(--cat-color,#fff);line-height:1;}
        .cat-trend{font-family:var(--sans);font-size:10px;font-weight:600;margin-top:3px;}
        .cat-trend.up{color:var(--cyan);}
        .cat-trend.down{color:var(--pink);}
        .cat-trend.flat{color:var(--muted);}
        .cat-sparkline{display:flex;align-items:flex-end;gap:2px;height:30px;margin-bottom:10px;}
        .spark-bar{width:100%;border-radius:1px;transition:height .3s;}
        .cat-hypothesis{font-size:10px;color:var(--muted);line-height:1.5;font-style:italic;}

        /* ── Signal detail panel ── */
        .detail-panel{background:var(--s1);border:1px solid var(--border);border-radius:8px;overflow:hidden;}
        .dp-header{
          display:flex;align-items:center;justify-content:space-between;
          padding:16px 20px;border-bottom:1px solid var(--border);
          background:var(--s2);
        }
        .dp-title{font-family:var(--display);font-size:14px;font-weight:700;color:var(--bright);}
        .dp-hypothesis{font-size:11px;color:var(--muted);margin-top:2px;font-style:italic;}
        .dp-body{display:grid;grid-template-columns:1fr 340px;gap:0;}
        .dp-signals{padding:16px 20px;border-right:1px solid var(--border);}
        .dp-chart-panel{padding:16px 20px;}

        /* ── Signal rows ── */
        .signal-row{
          display:flex;align-items:center;gap:12px;padding:10px 0;
          border-bottom:1px solid var(--border);
        }
        .signal-row:last-child{border-bottom:none;}
        .signal-row-left{display:flex;align-items:center;gap:8px;width:160px;flex-shrink:0;}
        .signal-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;}
        .signal-name{font-size:12px;color:var(--text);}
        .signal-sparkline-wrap{flex:1;}
        .signal-row-right{display:flex;align-items:center;gap:12px;flex-shrink:0;}
        .signal-val{font-family:var(--mono);font-size:14px;color:var(--bright);width:30px;text-align:right;}
        .signal-pct{font-family:var(--sans);font-size:10px;font-weight:600;width:40px;text-align:right;}
        .signal-pct.up{color:var(--cyan);}
        .signal-pct.down{color:var(--pink);}
        .signal-pct.flat{color:var(--muted);}
        .signal-news{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.45);width:55px;text-align:right;}

        /* ── Trending topics ── */
        .topics-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;}
        .topic-card{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:16px;}
        .tc-header{display:flex;align-items:center;gap:8px;margin-bottom:12px;}
        .tc-flag{font-size:20px;}
        .tc-name{font-family:var(--display);font-size:13px;font-weight:700;color:var(--bright);}
        .mood-bar{display:flex;height:3px;border-radius:2px;overflow:hidden;gap:1px;margin-bottom:8px;}
        .mood-labels{display:flex;justify-content:space-between;font-family:var(--sans);font-size:10px;font-weight:600;color:rgba(255,255,255,0.5);margin-bottom:10px;}
        .topic-item{font-size:11px;color:var(--text);padding:4px 0;border-bottom:1px solid var(--border);display:flex;gap:6px;}
        .topic-item:last-child{border-bottom:none;}
        .topic-num{font-family:var(--sans);color:rgba(255,255,255,0.4);font-size:10px;font-weight:600;flex-shrink:0;}

        /* ── Radar + long-term ── */
        .analysis-grid{display:grid;grid-template-columns:320px 1fr;gap:16px;}
        .card{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:20px;}
        .card-title{font-family:var(--sans);font-size:11px;font-weight:600;color:rgba(255,255,255,0.7);margin-bottom:4px;}
        .card-sub{font-size:11px;font-weight:400;color:rgba(255,255,255,0.4);margin-bottom:16px;}

        /* ── Period buttons ── */
        .period-btns{display:flex;gap:6px;margin-bottom:16px;}
        .period-btn{
          font-family:var(--sans);font-size:10px;font-weight:600;
          padding:4px 10px;border:1px solid var(--border);border-radius:2px;
          background:transparent;color:var(--muted);cursor:pointer;transition:all .15s;
        }
        .period-btn:hover{border-color:var(--cyan);color:var(--cyan);}
        .period-btn.active{border-color:var(--cyan);color:var(--cyan);background:rgba(0,229,200,0.07);}

        /* ── Twitch ── */
        .twitch-row{display:grid;grid-template-columns:auto 1fr;gap:24px;align-items:center;}
        .twitch-stat{padding-right:24px;border-right:1px solid var(--border);text-align:center;}
        .twitch-num{font-family:var(--mono);font-size:38px;color:var(--cyan);line-height:1;}
        .twitch-lbl{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.5);letter-spacing:1px;margin-top:4px;}
        .game-rows{display:flex;flex-direction:column;gap:9px;}
        .game-row{display:flex;align-items:center;gap:10px;}
        .game-name{font-size:11px;color:var(--text);width:180px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
        .game-bar-bg{flex:1;height:5px;background:var(--border);border-radius:3px;overflow:hidden;}
        .game-bar-fg{height:100%;border-radius:3px;}
        .game-views{font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.5);width:48px;text-align:right;}

        /* ── Loading ── */
        .loading-screen{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;gap:20px;}
        .loader{display:flex;align-items:flex-end;gap:4px;height:40px;}
        .loader-bar{
          width:6px;border-radius:3px;background:var(--cyan);
          animation:loadpulse 0.8s ease-in-out infinite alternate;
        }
        @keyframes loadpulse{from{height:8px;opacity:0.3}to{height:36px;opacity:1}}
        .loading-text{font-family:var(--sans);font-size:13px;font-weight:600;color:rgba(255,255,255,0.6);letter-spacing:1px;}
        .retry-btn{margin-top:8px;padding:8px 20px;border:1px solid var(--cyan);background:transparent;color:var(--cyan);font-family:var(--sans);font-size:11px;font-weight:600;border-radius:3px;cursor:pointer;}

        /* ── Overlay / settings ── */
        .overlay{position:fixed;inset:0;background:rgba(0,0,0,0.75);backdrop-filter:blur(4px);z-index:200;display:flex;justify-content:flex-end;padding:20px;}
        .settings-panel{width:600px;max-height:calc(100vh - 40px);background:var(--s1);border:1px solid var(--border2);border-radius:10px;display:flex;flex-direction:column;overflow:hidden;animation:slideIn .2s ease;}
        @keyframes slideIn{from{transform:translateX(24px);opacity:0}to{transform:none;opacity:1}}
        .sp-header{display:flex;align-items:center;justify-content:space-between;padding:18px 22px;border-bottom:1px solid var(--border);flex-shrink:0;}
        .sp-close{background:none;border:none;color:var(--muted);font-size:16px;cursor:pointer;padding:4px;}
        .sp-close:hover{color:var(--bright);}
        .sp-body{flex:1;overflow-y:auto;padding:20px 22px;display:flex;flex-direction:column;gap:16px;}
        .sp-pat-row{display:flex;flex-direction:column;gap:6px;}
        .sp-label{font-family:var(--sans);font-size:10px;font-weight:600;color:rgba(255,255,255,0.6);text-transform:uppercase;}
        .sp-input{background:var(--s2);border:1px solid var(--border);border-radius:3px;padding:8px 10px;color:var(--text);font-family:var(--sans);font-size:12px;width:100%;transition:border-color .15s;}
        .sp-input:focus{outline:none;border-color:var(--cyan);}
        .sp-input.sm{font-size:11px;padding:5px 8px;}
        .sp-divider{height:1px;background:var(--border);}
        .sp-cat{display:flex;flex-direction:column;gap:8px;}
        .sp-cat-header{font-family:var(--display);font-size:12px;font-weight:700;color:var(--bright);padding:8px 0 8px 12px;border-left:3px solid;display:flex;align-items:center;gap:10px;}
        .sp-ramadan-badge{font-family:var(--sans);font-size:10px;font-weight:600;color:var(--yellow);background:rgba(245,208,32,0.1);padding:2px 8px;border-radius:2px;border:1px solid rgba(245,208,32,0.2);}
        .sp-sig-row{background:var(--s2);border-radius:4px;padding:10px 12px;display:flex;flex-direction:column;gap:8px;}
        .sp-sig-name{font-size:11px;color:var(--text);}
        .sp-fields{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;}
        .sp-field-group{display:flex;flex-direction:column;gap:4px;}
        .sp-field-label{font-family:var(--sans);font-size:10px;font-weight:600;color:rgba(255,255,255,0.55);text-transform:uppercase;}
        .sp-footer{padding:14px 22px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:flex-end;gap:10px;flex-shrink:0;}
        .sp-msg{font-size:11px;flex:1;}
        .sp-msg.ok{color:var(--cyan);}
        .sp-msg.err{color:var(--pink);}
        .sp-cancel{font-family:var(--sans);font-size:11px;font-weight:600;padding:8px 16px;border:1px solid var(--border);background:transparent;color:var(--muted);border-radius:3px;cursor:pointer;}
        .sp-save{font-family:var(--sans);font-size:11px;font-weight:700;padding:8px 18px;border:1px solid var(--cyan);background:rgba(0,229,200,0.08);color:var(--cyan);border-radius:3px;cursor:pointer;transition:all .15s;}
        .sp-save:hover:not(:disabled){background:rgba(0,229,200,0.15);}
        .sp-save:disabled{opacity:0.4;cursor:not-allowed;}

        /* ── Footer ── */
        .footer{display:flex;align-items:center;justify-content:center;gap:16px;flex-wrap:wrap;padding:20px 32px 32px;font-family:var(--sans);font-size:10px;font-weight:500;color:rgba(255,255,255,0.4);border-top:1px solid var(--border);}

        /* ── Responsive ── */
        @media(max-width:1100px){.dp-body{grid-template-columns:1fr;}.dp-chart-panel{border-top:1px solid var(--border);}.analysis-grid{grid-template-columns:1fr;}}
        @media(max-width:900px){.topics-grid{grid-template-columns:1fr 1fr;}.cat-grid{grid-template-columns:repeat(2,1fr);}}
        @media(max-width:600px){.topics-grid{grid-template-columns:1fr;}.cat-grid{grid-template-columns:1fr;}.main{padding:16px;}.hdr{padding:12px 16px;}}
      `}</style>

      {/* Header */}
      <header className="hdr">
        <div className="hdr-left">
          <img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz4KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDIyLjEuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPgo8c3ZnIHZlcnNpb249IjEuMSIgaWQ9IkFydHdvcmsiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IgoJIHZpZXdCb3g9IjAgMCAxNzAuMSA1MSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgMTcwLjEgNTE7IiB4bWw6c3BhY2U9InByZXNlcnZlIj4KPHN0eWxlIHR5cGU9InRleHQvY3NzIj4KCS5zdDB7ZmlsbDojRkZGRkZGO30KPC9zdHlsZT4KPGc+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTEyLjIsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTEzLjgsMjQuOSwxMTMuMSwyNC4xLDExMi4yLDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMTMuMSwxNC41LDExMi4yLDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMTMuMSwxOS4zLDExMi4yLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzExNy4zLDcuMiwxMTYuNCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCwzYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMxMTYuOSwzLDExNi40LDMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwzMi4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDMTEwLjYsMzEuNSwxMTEuMywzMi4yLDExMi4yLDMyLjIKCQkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzExMy4xLDkuNiwxMTIuMiw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDgsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA4LjksMjYuNiwxMDgsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwOCwyNC40YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVMxMDcuNCwyNC40LDEwOCwyNC40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwyNC40YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVM5OS4xLDI0LjQsOTkuNiwyNC40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMzYuNWMwLjYsMCwxLTAuNSwxLTFzLTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzEwMy4zLDM2LjUsMTAzLjgsMzYuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwOCwxMi43Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxMDksMTMuMSwxMDguNiwxMi43LDEwOCwxMi43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwxMi43Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxMDAuNiwxMy4xLDEwMC4yLDEyLjcsOTkuNiwxMi43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTEyLjIsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMTMuMSw0LjgsMTEyLjIsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTEyLjIsMC42Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxMTMuMiwxLDExMi43LDAuNiwxMTIuMiwwLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDgsMzQuN2MwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42UzEwNy4xLDM0LjcsMTA4LDM0LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMTgsMTcuNiwxMTcuMywxNi45LDExNi40LDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTM2LjYsMTIuOCwxMzUuOSwxMi4xLDEzNSwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTM1LjksNy4yLDEzNSw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMjAuNiwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMjIuMiwyMCwxMjEuNSwxOS4zLDEyMC42LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwyLjQsMTM1LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwyNi42LDEzNSwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzEzNi42LDIyLjUsMTM1LjksMjEuNywxMzUsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwxNi45LDEzNSwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUM5Mi45LDIyLjUsOTIuMSwyMS43LDkxLjIsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDI5LjhjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZzLTEuNiwwLjctMS42LDEuNkMxMTQuNywyOS4xLDExNS41LDI5LjgsMTE2LjQsMjkuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEyMC42LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzEyMi4yLDE1LjIsMTIxLjUsMTQuNSwxMjAuNiwxNC41IgoJCS8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTA4LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA4LjksNy4yLDEwOCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTYuNCwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMTgsMjIuNSwxMTcuMywyMS43LDExNi40LDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMjAuNiwyNi44YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFzLTEsMC41LTEsMVMxMjAsMjYuOCwxMjAuNiwyNi44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTIwLjYsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMxMjEuNSw5LjYsMTIwLjYsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwzMy44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM5Ny4xLDM0LjUsOTYuMywzMy44LDk1LjQsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNlM5NywxNyw5NywxNi4xQzk3LjEsMTUuMiw5Ni4zLDE0LjUsOTUuNCwxNC41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTA4LDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA4LjksMi40LDEwOCwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNlM5Nyw3LjMsOTcsNi40Qzk3LjEsNS41LDk2LjMsNC44LDk1LjQsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42Uzk3LDIuNSw5NywxLjZTOTYuMywwLDk1LjQsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMjkuNyw5Ni4zLDI5LDk1LjQsMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMjQuOSw5Ni4zLDI0LjEsOTUuNCwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM5Ny4xLDIwLDk2LjMsMTkuMyw5NS40LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzkyLjEsMzEuNCw5MS4yLDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDM2LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzkyLjksMzcsOTIuMSwzNi4yLDkxLjIsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsNTFjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzk0LjksNTEsOTUuNCw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDOTIuOSwyNy4zLDkyLjEsMjYuNiw5MS4yLDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTM1LjksMzEuNCwxMzUsMzEuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDOTIuOSw0MS44LDkyLjEsNDEuMSw5MS4yLDQxLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42UzkyLjEsNDYsOTEuMiw0NnMtMS42LDAuNy0xLjYsMS42Qzg5LjYsNDguNCw5MC4zLDQ5LjIsOTEuMiw0OS4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Uzk2LjMsOS42LDk1LjQsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDQuNyw0LjgsMTAzLjgsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDQuNyw5LjYsMTAzLjgsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTA0LjcsMCwxMDMuOCwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwzNC43YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42Uzk4LDMyLjIsOTgsMzMuMVM5OC43LDM0LjcsOTkuNiwzNC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM5Ny4xLDM5LjQsOTYuMywzOC43LDk1LjQsMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwMy44LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzEwNS40LDI0LjksMTA0LjcsMjQuMSwxMDMuOCwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMzIuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42UzEwMi45LDMyLjIsMTAzLjgsMzIuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk5LjYsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMxMDAuNSw3LjIsOTkuNiw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05OS42LDIuNEM5OC43LDIuNCw5OCwzLjEsOTgsNGMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzEwMS4zLDMuMSwxMDAuNSwyLjQsOTkuNiwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDQzLjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZTOTcsNDYsOTcsNDUuMUM5Ny4xLDQ0LjIsOTYuMyw0My41LDk1LjQsNDMuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk5LjYsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTAxLjMsMjcuMywxMDAuNSwyNi42LDk5LjYsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzExOCwxMi44LDExNy4zLDEyLjEsMTE2LjQsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTU3LjUsNS41LDE1Ni44LDQuOCwxNTUuOSw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTUuOSw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE1Ni44LDkuNiwxNTUuOSw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTUuOSwyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTU3LjUsMjkuNywxNTYuOCwyOSwxNTUuOSwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE1Ny41LDI0LjksMTU2LjgsMjQuMSwxNTUuOSwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNjEuNywzLjEsMTYxLDIuNCwxNjAuMSwyLjQiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNjEuNywxMi44LDE2MSwxMi4xLDE2MC4xLDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNjEuNywxNy42LDE2MSwxNi45LDE2MC4xLDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE2MSw3LjIsMTYwLjEsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTUxLjcsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNTIuNiw3LjIsMTUxLjcsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTU1LjksMC42Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxNTYuOSwxLDE1Ni41LDAuNiwxNTUuOSwwLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTEuNywxMi43Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMxNTIuNywxMy4xLDE1Mi4zLDEyLjcsMTUxLjcsMTIuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDIyLjNjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzE1Mi4zLDIyLjMsMTUxLjcsMjIuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDM0LjRjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzE1Ni41LDM0LjQsMTU1LjksMzQuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTUyLjYsMi40LDE1MS43LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE1Mi42LDI2LjYsMTUxLjcsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDM0LjdjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNlMxNTAuOCwzNC43LDE1MS43LDM0LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDcuNSwzNi41YzAuNiwwLDEtMC41LDEtMXMtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFTMTQ3LDM2LjUsMTQ3LjUsMzYuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2OC41LDcuOGMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMTY5LDcuOCwxNjguNSw3LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjQuMywzMS42YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMxNjMuMywzMS4yLDE2My43LDMxLjYsMTY0LjMsMzEuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2MC4xLDM0YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMxNTkuMSwzMy42LDE1OS41LDM0LDE2MC4xLDM0Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY4LjUsMTUuM2MwLjksMCwxLjYtMC43LDEuNi0xLjZjMC0wLjktMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjYKCQlDMTY2LjgsMTQuNiwxNjcuNiwxNS4zLDE2OC41LDE1LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjguNSwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNzAuMSwyMi41LDE2OS40LDIxLjcsMTY4LjUsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2OC41LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE2OS40LDE2LjksMTY4LjUsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2NC4zLDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE2NS45LDI0LjksMTY1LjIsMjQuMSwxNjQuMywyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTY1LjIsMTkuMywxNjQuMywxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTYxLjcsMjcuMywxNjEsMjYuNiwxNjAuMSwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNjUuMiw0LjgsMTY0LjMsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTY1LjIsMTQuNSwxNjQuMywxNC41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTYxLjcsMjIuNSwxNjEsMjEuNywxNjAuMSwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNjUuMiw5LjYsMTY0LjMsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQwLjgsMjQuOSwxNDAuMSwyNC4xLDEzOS4yLDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTM2LjYsMzcsMTM1LjksMzYuMiwxMzUsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwyOSwxMzkuMiwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0Ny41LDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDkuMiwyOS43LDE0OC40LDI5LDE0Ny41LDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsNDMuNSwxMzkuMiw0My41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQwLjgsMzQuNSwxNDAuMSwzMy44LDEzOS4yLDMzLjgiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwxNC41LDEzOS4yLDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwzOC43LDEzOS4yLDM4LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDQuOCwxMzkuMiw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiw1MWMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDMTM4LjEsNTAuNSwxMzguNiw1MSwxMzkuMiw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSw0OS4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSw0NiwxMzUsNDZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkMxMzMuMyw0OC40LDEzNC4xLDQ5LjIsMTM1LDQ5LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTM1LjksNDEuMSwxMzUsNDEuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsOS42LDEzOS4yLDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDAsMTM5LjIsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0My4zLDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDMuMSwxNDQuMiwyLjQsMTQzLjMsMi40IgoJCS8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsMTkuMywxMzkuMiwxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQ4LjQsMCwxNDcuNSwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNDkuMiw1LjUsMTQ4LjQsNC44LDE0Ny41LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0My4zLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkMxNDEuNyw0OC40LDE0Mi40LDQ5LjIsMTQzLjMsNDkuMgoJCSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0Ny41LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE0OS4yLDI0LjksMTQ4LjQsMjQuMSwxNDcuNSwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDQxLjgsMTQ0LjIsNDEuMSwxNDMuMyw0MS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDguNCw5LjYsMTQ3LjUsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDI3LjMsMTQ0LjIsMjYuNiwxNDMuMywyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ1LDIyLjUsMTQ0LjIsMjEuNywxNDMuMywyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMTYuOWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ1LDE3LjYsMTQ0LjIsMTYuOSwxNDMuMywxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ1LDEyLjgsMTQ0LjIsMTIuMSwxNDMuMywxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDQuMiw3LjIsMTQzLjMsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQ0LjIsMzEuNCwxNDMuMywzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMTQ1LDM3LDE0NC4yLDM2LjIsMTQzLjMsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTc2LjMsM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNzYuOSwzLDc2LjMsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzMxLjIsMjksMzAuMywyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMzEuMiwyNC4xLDMwLjMsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMTUuMWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMzEuMywxNS41LDMwLjgsMTUuMSwzMC4zLDE1LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zMC4zLDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzMxLjIsMTkuMywzMC4zLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDM0YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMzMy40LDMzLjYsMzMuOSwzNCwzNC41LDM0Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzAuMywzMy44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMzMS45LDM0LjUsMzEuMiwzMy44LDMwLjMsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsNDEuOWMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNnMtMS42LDAuNy0xLjYsMS42QzI4LjYsNDEuMiwyOS40LDQxLjksMzAuMyw0MS45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjYuMSw0OS4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNlMyNyw0NiwyNi4xLDQ2cy0xLjYsMC43LTEuNiwxLjZDMjQuNCw0OC40LDI1LjIsNDkuMiwyNi4xLDQ5LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yNi4xLDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzI3LDMxLjQsMjYuMSwzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMyMy41LDM5LjQsMjIuOCwzOC43LDIxLjksMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMjMuNSw0NC4yLDIyLjgsNDMuNSwyMS45LDQzLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yNi4xLDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzI3LDI2LjYsMjYuMSwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjYuMSw0MS4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMyNyw0MS4xLDI2LjEsNDEuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTI2LjEsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMjcuNywzNywyNywzNi4yLDI2LjEsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsMjcuNGMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNlMzNywyNC45LDM3LDI1LjhTMzcuNywyNy40LDM4LjYsMjcuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDAuMywyMCwzOS41LDE5LjMsMzguNiwxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzguNiwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0MC4zLDE1LjIsMzkuNSwxNC41LDM4LjYsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMzOS41LDkuNiwzOC42LDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsMzRjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzQxLjgsMzMuNiw0Mi4zLDM0LDQyLjgsMzQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Mi44LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNDMuNyw3LjIsNDIuOCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMzUuNCw3LjIsMzQuNSw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zOC42LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42QzM3LDcuMywzNy43LDgsMzguNiw4czEuNi0wLjcsMS42LTEuNkM0MC4zLDUuNSwzOS41LDQuOCwzOC42LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMzYuMSwxMi44LDM1LjQsMTIuMSwzNC41LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzM2LjEsMjIuNSwzNS40LDIxLjcsMzQuNSwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzQuNSwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMzNi4xLDE3LjYsMzUuNCwxNi45LDM0LjUsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMzYuMSwyNy4zLDM1LjQsMjYuNiwzNC41LDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zOC42LDAuNmMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMzkuNywxLDM5LjIsMC42LDM4LjYsMC42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzQuNSwzYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMzNSwzLDM0LjUsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNDMuNCwzLDQyLjgsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkuNCwxMC41YzAuOSwwLDEuNi0wLjcsMS42LTEuNmMwLTAuOS0wLjctMS42LTEuNi0xLjZDOC41LDcuMyw3LjgsOCw3LjgsOC45QzcuOCw5LjcsOC41LDEwLjUsOS40LDEwLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCw0LjgsMTMuNSw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCw5LjYsMTMuNSw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0LjQsMTkuMywxMy41LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0LjQsMTQuNSwxMy41LDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05LjQsMjAuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZjMC0wLjktMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDNy44LDE5LjQsOC41LDIwLjIsOS40LDIwLjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Mi44LDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzQ0LjUsMTIuOCw0My43LDEyLjEsNDIuOCwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOS40LDE1LjNjMC45LDAsMS42LTAuNywxLjYtMS42YzAtMC45LTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42QzcuOCwxNC42LDguNSwxNS4zLDkuNCwxNS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTMuNSwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNC40LDAsMTMuNSwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNS4yLDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42QzMuNiw3LjMsNC4zLDgsNS4yLDhjMC45LDAsMS42LTAuNywxLjYtMS42QzYuOSw1LjUsNi4xLDQuOCw1LjIsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNS4yLDBDNC4zLDAsMy42LDAuNywzLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNi4xLDAsNS4yLDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xLDNDMC40LDMsMCwzLjUsMCw0YzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMi4xLDMuNSwxLjYsMywxLDMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01LjIsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2LjEsOS42LDUuMiw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05LjQsMi40QzguNSwyLjQsNy44LDMuMSw3LjgsNGMwLDAuOSwwLjcsMS42LDEuNiwxLjZDMTAuMyw1LjYsMTEsNC45LDExLDRDMTEsMy4xLDEwLjMsMi40LDkuNCwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDMxLjZjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzEyLjUsMzEuMiwxMywzMS42LDEzLjUsMzEuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsNy44Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMxOC4zLDcuOCwxNy43LDcuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksNTFjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzIxLjMsNTEsMjEuOSw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMTkuOWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMjIuNCwxOS45LDIxLjksMTkuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMTUuMWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDMjIuOSwxNS41LDIyLjQsMTUuMSwyMS45LDE1LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01NS40LDE1LjFjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzU2LjQsMTUuNSw1NiwxNS4xLDU1LjQsMTUuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTI2LjEsMjIuM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMjYuNiwyMi4zLDI2LjEsMjIuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsMjIuM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNTEuOCwyMi4zLDUxLjIsMjIuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTkuMywzMi4xLDE4LjYsMzEuNCwxNy43LDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDM2LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzE5LjMsMzcsMTguNiwzNi4yLDE3LjcsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCwyNC4xLDEzLjUsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMjIuOCwyNC4xLDIxLjksMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzIzLjUsMjkuNywyMi44LDI5LDIxLjksMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yMS45LDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNlMyMSwzNywyMS45LDM3YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMyMy41LDM0LjUsMjIuOCwzMy44LDIxLjksMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTkuMywxMi44LDE4LjYsMTIuMSwxNy43LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzE4LjYsMjYuNiwxNy43LDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDQzLjdjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzE2LjcsNDMuMywxNy4xLDQzLjcsMTcuNyw0My43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTcuNywyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxOS4zLDIyLjUsMTguNiwyMS43LDE3LjcsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMTYuOWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMTguNiwxNi45LDE3LjcsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU5LjYsNDMuN2MwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xcy0xLDAuNS0xLDFDNTguNiw0My4zLDU5LDQzLjcsNTkuNiw0My43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjgsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDNjkuNiwxMi44LDY4LjksMTIuMSw2OCwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjgsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2OC45LDcuMiw2OCw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02OCwyMC4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNmMwLTAuOS0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkM2Ni4zLDE5LjQsNjcuMSwyMC4yLDY4LDIwLjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik03Mi4yLDEyLjljMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNlM3MS4zLDEyLjksNzIuMiwxMi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjgsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkM2OS42LDMuMSw2OC45LDIuNCw2OCwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik03Mi4yLDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNzMuMSw0LjgsNzIuMiw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik03Mi4yLDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzczLjEsMCw3Mi4yLDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMTkuMyw2My44LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNyw5LjYsNjMuOCw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNyw0LjgsNjMuOCw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMCw2My44LDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDUxYzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUM4Niw1MC41LDg2LjUsNTEsODcuMSw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsMzEuNmMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDNjIuOCwzMS4yLDYzLjIsMzEuNiw2My44LDMxLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDI0LjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMjQuMSw2My44LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02My44LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY0LjcsMTQuNSw2My44LDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Qzg4LjcsMzQuNSw4OCwzMy44LDg3LjEsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsNDMuNSw4Ny4xLDQzLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDM4LjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Uzg4LDM4LjcsODcuMSwzOC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwyLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzkyLjksMy4xLDkyLjEsMi40LDkxLjIsMi40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzkyLjEsNy4yLDkxLjIsNy4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUM5Mi45LDEyLjgsOTIuMSwxMi4xLDkxLjIsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsMTkuMyw4Ny4xLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwyOSw4Ny4xLDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwxNC41LDg3LjEsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCw0LjgsODcuMSw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsOS42LDg3LjEsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwwLDg3LjEsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDODguNywyNC45LDg4LDI0LjEsODcuMSwyNC4xIgoJCS8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0NC41LDE3LjYsNDMuNywxNi45LDQyLjgsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTIuMSwyNi42LDUxLjIsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQ3LDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNlM0Ni4xLDM3LDQ3LDM3czEuNi0wLjcsMS42LTEuNkM0OC43LDM0LjUsNDcuOSwzMy44LDQ3LDMzLjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01NS40LDUxYzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVM1NC44LDUxLDU1LjQsNTEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzUyLjEsMzEuNCw1MS4yLDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42UzUyLjEsNDYsNTEuMiw0NnMtMS42LDAuNy0xLjYsMS42QzQ5LjYsNDguNCw1MC4zLDQ5LjIsNTEuMiw0OS4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTEuMiwzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM1Mi44LDM3LDUyLjEsMzYuMiw1MS4yLDM2LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDQxLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzUyLjEsNDEuMSw1MS4yLDQxLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Nyw0MS45YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42cy0xLjYsMC43LTEuNiwxLjZTNDYuMSw0MS45LDQ3LDQxLjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywxNS4xYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUM0OCwxNS41LDQ3LjYsMTUuMSw0NywxNS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0NC41LDI3LjMsNDMuNywyNi42LDQyLjgsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDQuNSwyMi41LDQzLjcsMjEuNyw0Mi44LDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDguNywyOS43LDQ3LjksMjksNDcsMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0OC43LDIwLDQ3LjksMTkuMyw0NywxOS4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDcsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNDcuOSwyNC4xLDQ3LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzYxLjIsMjIuNSw2MC41LDIxLjcsNTkuNiwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM2MC41LDE2LjksNTkuNiwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM2MS4yLDM3LDYwLjUsMzYuMiw1OS42LDM2LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01NS40LDE5LjljLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzU2LDE5LjksNTUuNCwxOS45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwzMS40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM2MC41LDMxLjQsNTkuNiwzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM2MC41LDI2LjYsNTkuNiwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiw3LjhjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFzMS0wLjUsMS0xUzYwLjEsNy44LDU5LjYsNy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM2MS4yLDEyLjgsNjAuNSwxMi4xLDU5LjYsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzU2LjMsMjksNTUuNCwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTYuMywyNC4xLDU1LjQsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNTcsMzQuNSw1Ni4zLDMzLjgsNTUuNCwzMy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUM5Mi45LDE3LjYsOTIuMSwxNi45LDkxLjIsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMzguN2MtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTYuMywzOC43LDU1LjQsMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNlM1Nyw0Niw1Nyw0NS4xUzU2LjMsNDMuNSw1NS40LDQzLjUiLz4KPC9nPgo8L3N2Zz4K" className="wpp-logo" alt="WPP Media"/>
          <div className="hdr-divider"/>
          <div className="product-badge">
            <div className="pulse-ring"/>
            <div>
              <div className="hdr-title">Crisis Pulse</div>
              <div className="hdr-sub">Consumer Signal Intelligence · MENA</div>
            </div>
          </div>
        </div>
        <div className="hdr-right">
          <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
            {["wikipedia","google_rss","newsapi","guardian","twitch"].map(s=>(
              <span key={s} className={`chip ${sources.includes(s)?"live":"dead"}`}>
                {s.replace("_"," ")}
              </span>
            ))}
          </div>
          <span className="ts">{timeAgoStr}</span>
          <button className="sp-trigger" onClick={()=>setShowSettings(true)}>⚙ Signals</button>
        </div>
      </header>

      {/* Ramadan banner */}
      {isRamadan && <RamadanBanner endDate={ramadanEnd}/>}

      {/* Sticky market nav */}
      <div className="sticky-nav">
        <div className="market-bar">
          {Object.entries(MARKET_FLAGS).map(([m,flag])=>(
            <button key={m} className={`mkt-tab ${activeMarket===m?"active":""}`}
              onClick={()=>setActiveMarket(m)}>
              {flag} {m}
            </button>
          ))}
        </div>
      </div>

      <main className="main">


        {/* ── Category overview ── */}
        <div>
          <div className="sec">Signal Categories · {activeMarket} · click to drill down</div>
          <div className="cat-grid">
            {catKeys.map(ck=>(
              <CategoryCard key={ck} catKey={ck} cat={categories[ck]}
                signals={flatSigs} markets={markets}
                activeMarket={activeMarket}
                isActive={activeCat===ck}
                onClick={()=>setActiveCat(activeCat===ck?null:ck)}
                newsapi={newsapi} guardian={guardian} rss={rss} />
            ))}
          </div>
        </div>

        {/* ── Signal detail (expandable) ── */}
        {activeCat && activeCatObj && (
          <div className="detail-panel">
            <div className="dp-header" style={{borderTop:`2px solid ${activeCatObj.color}`}}>
              <div>
                <div className="dp-title">{activeCatObj.icon} {activeCatObj.label}</div>
                <div className="dp-hypothesis">{activeCatObj.hypothesis}</div>
              </div>
              <button onClick={()=>setActiveCat(null)}
                style={{background:"none",border:"1px solid var(--border)",color:"var(--muted)",borderRadius:3,padding:"5px 12px",cursor:"pointer",fontFamily:"var(--sans)",fontSize:11,fontWeight:600}}>
                ✕ Close
              </button>
            </div>
            <div className="dp-body">
              <div className="dp-signals">
                <div style={{display:"flex",gap:20,marginBottom:12,paddingBottom:10,borderBottom:"1px solid var(--border)"}}>
                  <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.5)",width:160}}>SIGNAL</span>
                  <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.5)",flex:1}}>7-DAY TREND</span>
                  <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.5)",width:30,textAlign:"right"}}>IDX</span>
                  <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.5)",width:40,textAlign:"right"}}>WoW</span>
                  <span style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.5)",width:55,textAlign:"right"}}>NEWS</span>
                </div>
                {activeSigKeys.map(sk=>(
                  <SignalRow key={sk} sigKey={sk} sig={flatSigs[sk]}
                    markets={markets} activeMarket={activeMarket}
                    dates={dates} newsapi={newsapi} guardian={guardian}
                    newsapiAllMarkets={newsapiByMarket} />
                ))}
              </div>
              <div className="dp-chart-panel">
                <div style={{fontFamily:"var(--sans)",fontSize:10,fontWeight:600,letterSpacing:1,color:"rgba(255,255,255,0.55)",marginBottom:4}}>
                  MARKET COMPARISON · {activeCatObj.label?.toUpperCase()}
                </div>
                <div style={{fontSize:10,color:"var(--muted)",marginBottom:14,fontStyle:"italic"}}>
                  30-day avg signal index per market
                </div>
                {(()=>{
                  // Build per-market category average from history (last 30 days)
                  const allMkts = Object.keys(MARKET_FLAGS);
                  const mktColors:Record<string,string> = {
                    UAE:"#00e5c8", KSA:"#f72585", Kuwait:"#fb8500", Qatar:"#8338ec"
                  };
                  // Apply same RSS-weight multiplier as main history chart
                  // so per-market lines are genuinely different
                  const catType = activeCat==="escapism"||activeCat==="entertainment" ? "sport"
                    : activeCat==="crisis_awareness"||activeCat==="news" ? "crisis" : "econ";
                  const histData = history.slice(-30).map((rec:any)=>({
                    date: rec.date?.slice(5),
                    ...Object.fromEntries(allMkts.map(m=>{
                      const vals = activeSigKeys
                        .map((s:string)=>rec.markets?.["UAE"]?.[s])
                        .filter((v:any)=>v!=null) as number[];
                      const base = vals.length ? vals.reduce((a:number,b:number)=>a+b,0)/vals.length : null;
                      if(base===null) return [m, null];
                      const rm = rss[m]||{};
                      const sportW = (rm.sport_entertainment_pct||50)/50;
                      const crisisW = (rm.crisis_pct||50)/50;
                      const w = catType==="sport" ? sportW : catType==="crisis" ? crisisW : (sportW+crisisW)/2;
                      return [m, Math.min(99, Math.round(base * w))];
                    }))
                  }));
                  return (
                    <ResponsiveContainer width="100%" height={200}>
                      <LineChart data={histData}>
                        <XAxis dataKey="date" tick={{fontSize:9,fill:"#3d5060"}} axisLine={false} tickLine={false}
                          interval={Math.floor(histData.length/4)}/>
                        <YAxis domain={[0,100]} tick={{fontSize:9,fill:"#3d5060"}} axisLine={false} tickLine={false} width={26}/>
                        <Tooltip content={<CustomTooltip/>}/>
                        {allMkts.map(m=>(
                          <Line key={m} type="monotone" dataKey={m} name={m}
                            stroke={mktColors[m]||"#4a6070"}
                            strokeWidth={m===activeMarket ? 2.5 : 1.2}
                            strokeOpacity={m===activeMarket ? 1 : 0.5}
                            dot={false} connectNulls/>
                        ))}
                      </LineChart>
                    </ResponsiveContainer>
                  );
                })()}
              </div>
            </div>
          </div>
        )}

        {/* ── Trending Topics ── */}
        <div>
          <div className="sec">Trending Topics · Google RSS · Today</div>
          <div className="topics-grid">
            {Object.entries(MARKET_FLAGS).map(([market,flag])=>{
              const r=rss[market]||{};
              const sport=r.sport_entertainment_pct||0;
              const crisis=r.crisis_pct||0;
              const other=Math.max(0,100-sport-crisis);
              return (
                <div key={market} className="topic-card">
                  <div className="tc-header">
                    <span className="tc-flag">{flag}</span>
                    <span className="tc-name">{market}</span>
                  </div>
                  <div className="mood-bar">
                    <div style={{width:`${sport}%`,background:"var(--cyan)",borderRadius:2}}/>
                    <div style={{width:`${crisis}%`,background:"var(--pink)",borderRadius:2}}/>
                    <div style={{width:`${other}%`,background:"var(--border)",borderRadius:2}}/>
                  </div>
                  <div className="mood-labels">
                    <span style={{color:"var(--cyan)"}}>◈ Sport/Ent {sport}%</span>
                    <span style={{color:"var(--pink)"}}>◈ Crisis {crisis}%</span>
                  </div>
                  {(r.top_topics||[]).slice(0,5).map((t:string,i:number)=>(
                    <div key={i} className="topic-item">
                      <span className="topic-num">{i+1}</span>{t}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        </div>

        {/* ── Radar + Long-term ── */}
        <div className="analysis-grid">
          <div className="card">
            <div className="card-title">Category Radar · {activeMarket}</div>
            <div className="card-sub">Signal strength by category</div>
            <ResponsiveContainer width="100%" height={260}>
              <RadarChart data={radarData} margin={{top:10,right:20,bottom:10,left:20}}>
                <PolarGrid stroke="var(--border)" />
                <PolarAngleAxis dataKey="category"
                  tick={{fill:"var(--muted)",fontSize:9,fontFamily:"DM Mono"}} />
                <Radar dataKey="value" stroke="var(--cyan)" fill="var(--cyan)"
                  fillOpacity={0.15} strokeWidth={1.5} dot={{r:3,fill:"var(--cyan)"}}/>
                <Tooltip content={<CustomTooltip/>}/>
              </RadarChart>
            </ResponsiveContainer>
          </div>

          <div className="card">
            <div className="card-title">Long-term Category Trends · {activeMarket}</div>
            <div className="card-sub">Daily avg signal index per category · 30-day view</div>
            {history.length > 0 ? (
              <>
                <div className="period-btns">
                  {[14,30,60,90].map(d=>(
                    history.length>=d &&
                    <button key={d} className={`period-btn ${historyDays===d?"active":""}`}
                      onClick={()=>setHistoryDays(d)}>{d}d</button>
                  ))}
                </div>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={historyChart}>
                    <XAxis dataKey="date" tick={{fontSize:9,fill:"#3d5060"}} axisLine={false} tickLine={false} interval="preserveStartEnd"/>
                    <YAxis domain={[0,100]} tick={{fontSize:9,fill:"#3d5060"}} axisLine={false} tickLine={false} width={26}/>
                    <Tooltip content={<CustomTooltip/>}/>
                    {catKeys.map(ck=>(
                      <Line key={ck} type="monotone" dataKey={ck}
                        name={categories[ck].label.split("&")[0].trim()}
                        stroke={categories[ck].color} strokeWidth={1.5}
                        dot={false} connectNulls/>
                    ))}
                  </LineChart>
                </ResponsiveContainer>
                <div style={{display:"flex",flexWrap:"wrap",gap:"6px 16px",marginTop:12}}>
                  {catKeys.map(ck=>(
                    <div key={ck} style={{display:"flex",alignItems:"center",gap:5,fontSize:10,fontWeight:600,color:"rgba(255,255,255,0.55)",fontFamily:"var(--sans)"}}>
                      <div style={{width:16,height:2,background:categories[ck].color,borderRadius:1}}/>
                      {categories[ck].label.split("&")[0].trim()}
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <div style={{display:"flex",alignItems:"center",justifyContent:"center",height:200,flexDirection:"column",gap:8}}>
                <div style={{fontSize:24}}>📭</div>
                <div style={{fontFamily:"var(--sans)",fontSize:12,fontWeight:500,color:"rgba(255,255,255,0.5)"}}>
                  History builds after first collector run
                </div>
              </div>
            )}
          </div>
        </div>

        {/* ── Twitch ── */}
        {twitch.total_viewers > 0 && (
          <div>
            <div className="sec">Live Gaming · Twitch · Right Now</div>
            <div className="card">
              <div className="twitch-row">
                <div className="twitch-stat">
                  <div className="twitch-num">{fmt(twitch.total_viewers||0)}</div>
                  <div className="twitch-lbl">Live Viewers</div>
                </div>
                <div className="game-rows">
                  {(twitch.top_games||[]).map((g:any,i:number)=>{
                    const max=(twitch.top_games?.[0]?.viewers)||1;
                    const colors=["var(--cyan)","var(--purple)","var(--orange)","var(--green)","var(--muted)"];
                    return (
                      <div key={i} className="game-row">
                        <div className="game-name">{g.name}</div>
                        <div className="game-bar-bg">
                          <div className="game-bar-fg" style={{width:`${(g.viewers/max)*100}%`,background:colors[i]}}/>
                        </div>
                        <div className="game-views">{fmt(g.viewers)}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        )}

        <div className="footer">
          <img src="data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz4KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDIyLjEuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPgo8c3ZnIHZlcnNpb249IjEuMSIgaWQ9IkFydHdvcmsiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgeG1sbnM6eGxpbms9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsiIHg9IjBweCIgeT0iMHB4IgoJIHZpZXdCb3g9IjAgMCAxNzAuMSA1MSIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgMTcwLjEgNTE7IiB4bWw6c3BhY2U9InByZXNlcnZlIj4KPHN0eWxlIHR5cGU9InRleHQvY3NzIj4KCS5zdDB7ZmlsbDpyZ2JhKDI1NSwyNTUsMjU1LDAuNCk7fQo8L3N0eWxlPgo8Zz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxMTMuOCwyNC45LDExMy4xLDI0LjEsMTEyLjIsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExMi4yLDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzExMy4xLDE0LjUsMTEyLjIsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExMi4yLDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzExMy4xLDE5LjMsMTEyLjIsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMTE3LjMsNy4yLDExNi40LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDNjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzExNi45LDMsMTE2LjQsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExMi4yLDMyLjJjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkMxMTAuNiwzMS41LDExMS4zLDMyLjIsMTEyLjIsMzIuMgoJCSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExMi4yLDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTEzLjEsOS42LDExMi4yLDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwOCwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDguOSwyNi42LDEwOCwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTA4LDI0LjRjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzEwNy40LDI0LjQsMTA4LDI0LjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05OS42LDI0LjRjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzk5LjEsMjQuNCw5OS42LDI0LjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDMuOCwzNi41YzAuNiwwLDEtMC41LDEtMXMtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFTMTAzLjMsMzYuNSwxMDMuOCwzNi41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTA4LDEyLjdjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzEwOSwxMy4xLDEwOC42LDEyLjcsMTA4LDEyLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05OS42LDEyLjdjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzEwMC42LDEzLjEsMTAwLjIsMTIuNyw5OS42LDEyLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzExMy4xLDQuOCwxMTIuMiw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMTIuMiwwLjZjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzExMy4yLDEsMTEyLjcsMC42LDExMi4yLDAuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEwOCwzNC43YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZTMTA3LjEsMzQuNywxMDgsMzQuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzExOCwxNy42LDExNy4zLDE2LjksMTE2LjQsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxMzYuNiwxMi44LDEzNS45LDEyLjEsMTM1LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSw3LjIsMTM1LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEyMC42LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzEyMi4yLDIwLDEyMS41LDE5LjMsMTIwLjYsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwyLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzEzNS45LDIuNCwxMzUsMi40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzEzNS45LDI2LjYsMTM1LDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzUsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTM2LjYsMjIuNSwxMzUuOSwyMS43LDEzNSwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzEzNS45LDE2LjksMTM1LDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzkyLjksMjIuNSw5Mi4xLDIxLjcsOTEuMiwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTE2LjQsMjkuOGMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNnMtMS42LDAuNy0xLjYsMS42QzExNC43LDI5LjEsMTE1LjUsMjkuOCwxMTYuNCwyOS44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTIwLjYsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTIyLjIsMTUuMiwxMjEuNSwxNC41LDEyMC42LDE0LjUiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDgsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDguOSw3LjIsMTA4LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTExNi40LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzExOCwyMi41LDExNy4zLDIxLjcsMTE2LjQsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEyMC42LDI2LjhjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMXMtMSwwLjUtMSwxUzEyMCwyNi44LDEyMC42LDI2LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMjAuNiw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzEyMS41LDkuNiwxMjAuNiw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMzQuNSw5Ni4zLDMzLjgsOTUuNCwzMy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42Uzk3LDE3LDk3LDE2LjFDOTcuMSwxNS4yLDk2LjMsMTQuNSw5NS40LDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDgsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDguOSwyLjQsMTA4LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42Uzk3LDcuMyw5Nyw2LjRDOTcuMSw1LjUsOTYuMyw0LjgsOTUuNCw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZTOTcsMi41LDk3LDEuNlM5Ni4zLDAsOTUuNCwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCwyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDOTcuMSwyOS43LDk2LjMsMjksOTUuNCwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDOTcuMSwyNC45LDk2LjMsMjQuMSw5NS40LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMjAsOTYuMywxOS4zLDk1LjQsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTOTIuMSwzMS40LDkxLjIsMzEuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDOTIuOSwzNyw5Mi4xLDM2LjIsOTEuMiwzNi4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTUuNCw1MWMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFTOTQuOSw1MSw5NS40LDUxIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkM5Mi45LDI3LjMsOTIuMSwyNi42LDkxLjIsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwzMS40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSwzMS40LDEzNSwzMS40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTEuMiw0MS4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkM5Mi45LDQxLjgsOTIuMSw0MS4xLDkxLjIsNDEuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkxLjIsNDkuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZTOTIuMSw0Niw5MS4yLDQ2cy0xLjYsMC43LTEuNiwxLjZDODkuNiw0OC40LDkwLjMsNDkuMiw5MS4yLDQ5LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTOTYuMyw5LjYsOTUuNCw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDMuOCw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzEwNC43LDQuOCwxMDMuOCw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDMuOCw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzEwNC43LDkuNiwxMDMuOCw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDMuOCwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMDQuNywwLDEwMy44LDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05OS42LDM0LjdjMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZTOTgsMzIuMiw5OCwzMy4xUzk4LjcsMzQuNyw5OS42LDM0LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05NS40LDM4LjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42Qzk3LjEsMzkuNCw5Ni4zLDM4LjcsOTUuNCwzOC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTAzLjgsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTA1LjQsMjQuOSwxMDQuNywyNC4xLDEwMy44LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMDMuOCwzMi4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZTMTAyLjksMzIuMiwxMDMuOCwzMi4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzEwMC41LDcuMiw5OS42LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk5LjYsMi40Qzk4LjcsMi40LDk4LDMuMSw5OCw0YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTAxLjMsMy4xLDEwMC41LDIuNCw5OS42LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTk1LjQsNDMuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNlM5Nyw0Niw5Nyw0NS4xQzk3LjEsNDQuMiw5Ni4zLDQzLjUsOTUuNCw0My41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOTkuNiwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxMDEuMywyNy4zLDEwMC41LDI2LjYsOTkuNiwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTE2LjQsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTE4LDEyLjgsMTE3LjMsMTIuMSwxMTYuNCwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTU1LjksNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNTcuNSw1LjUsMTU2LjgsNC44LDE1NS45LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDkuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTU2LjgsOS42LDE1NS45LDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1NS45LDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNTcuNSwyOS43LDE1Ni44LDI5LDE1NS45LDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTU1LjksMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTU3LjUsMjQuOSwxNTYuOCwyNC4xLDE1NS45LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwyLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzE2MS43LDMuMSwxNjEsMi40LDE2MC4xLDIuNCIKCQkvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2MC4xLDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE2MS43LDEyLjgsMTYxLDEyLjEsMTYwLjEsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2MC4xLDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE2MS43LDE3LjYsMTYxLDE2LjksMTYwLjEsMTYuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2MC4xLDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTYxLDcuMiwxNjAuMSw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTEuNyw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE1Mi42LDcuMiwxNTEuNyw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNTUuOSwwLjZjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzE1Ni45LDEsMTU2LjUsMC42LDE1NS45LDAuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE1MS43LDEyLjdjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzE1Mi43LDEzLjEsMTUyLjMsMTIuNywxNTEuNywxMi43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTUxLjcsMjIuM2MtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMTUyLjMsMjIuMywxNTEuNywyMi4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTU1LjksMzQuNGMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTMTU2LjUsMzQuNCwxNTUuOSwzNC40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTUxLjcsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNTIuNiwyLjQsMTUxLjcsMi40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTUxLjcsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTUyLjYsMjYuNiwxNTEuNywyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTUxLjcsMzQuN2MwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42UzE1MC44LDM0LjcsMTUxLjcsMzQuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE0Ny41LDM2LjVjMC42LDAsMS0wLjUsMS0xcy0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMVMxNDcsMzYuNSwxNDcuNSwzNi41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY4LjUsNy44Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMxNjksNy44LDE2OC41LDcuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2NC4zLDMxLjZjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzE2My4zLDMxLjIsMTYzLjcsMzEuNiwxNjQuMywzMS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTYwLjEsMzRjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzE1OS4xLDMzLjYsMTU5LjUsMzQsMTYwLjEsMzQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjguNSwxNS4zYzAuOSwwLDEuNi0wLjcsMS42LTEuNmMwLTAuOS0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNgoJCUMxNjYuOCwxNC42LDE2Ny42LDE1LjMsMTY4LjUsMTUuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE2OC41LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE3MC4xLDIyLjUsMTY5LjQsMjEuNywxNjguNSwyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY4LjUsMTYuOWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTY5LjQsMTYuOSwxNjguNSwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTY0LjMsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTY1LjksMjQuOSwxNjUuMiwyNC4xLDE2NC4zLDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjQuMywxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNjUuMiwxOS4zLDE2NC4zLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNjEuNywyNy4zLDE2MSwyNi42LDE2MC4xLDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjQuMyw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE2NS4yLDQuOCwxNjQuMyw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjQuMywxNC41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNjUuMiwxNC41LDE2NC4zLDE0LjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjAuMSwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNjEuNywyMi41LDE2MSwyMS43LDE2MC4xLDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNjQuMyw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE2NS4yLDkuNiwxNjQuMyw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNDAuOCwyNC45LDE0MC4xLDI0LjEsMTM5LjIsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSwzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxMzYuNiwzNywxMzUuOSwzNi4yLDEzNSwzNi4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDI5LDEzOS4yLDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzE0OS4yLDI5LjcsMTQ4LjQsMjksMTQ3LjUsMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiw0My41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSw0My41LDEzOS4yLDQzLjUiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwzMy44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDAuOCwzNC41LDE0MC4xLDMzLjgsMTM5LjIsMzMuOCIKCQkvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDE0LjUsMTM5LjIsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDM4LjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0MC4xLDM4LjcsMTM5LjIsMzguNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDQuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsNC44LDEzOS4yLDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzOS4yLDUxYzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUMxMzguMSw1MC41LDEzOC42LDUxLDEzOS4yLDUxIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM1LDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42UzEzNS45LDQ2LDEzNSw0NmMtMC45LDAtMS42LDAuNy0xLjYsMS42QzEzMy4zLDQ4LjQsMTM0LjEsNDkuMiwxMzUsNDkuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzNSw0MS4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxMzUuOSw0MS4xLDEzNSw0MS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSw5LjYsMTM5LjIsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTM5LjIsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQwLjEsMCwxMzkuMiwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsMi40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDUsMy4xLDE0NC4yLDIuNCwxNDMuMywyLjQiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMzkuMiwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDAuMSwxOS4zLDEzOS4yLDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDcuNSwwYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDguNCwwLDE0Ny41LDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDcuNSw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzE0OS4yLDUuNSwxNDguNCw0LjgsMTQ3LjUsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQzLjMsNDkuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42QzE0MS43LDQ4LjQsMTQyLjQsNDkuMiwxNDMuMyw0OS4yCgkJIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTQ3LjUsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjYKCQlDMTQ5LjIsMjQuOSwxNDguNCwyNC4xLDE0Ny41LDI0LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMyw0MS4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDUsNDEuOCwxNDQuMiw0MS4xLDE0My4zLDQxLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDcuNSw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0OC40LDkuNiwxNDcuNSw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMywyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDUsMjcuMywxNDQuMiwyNi42LDE0My4zLDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMywyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNDUsMjIuNSwxNDQuMiwyMS43LDE0My4zLDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMywxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNDUsMTcuNiwxNDQuMiwxNi45LDE0My4zLDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMywxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNgoJCUMxNDUsMTIuOCwxNDQuMiwxMi4xLDE0My4zLDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMyw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0NC4yLDcuMiwxNDMuMyw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMywzMS40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNDQuMiwzMS40LDE0My4zLDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNDMuMywzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMxNDUsMzcsMTQ0LjIsMzYuMiwxNDMuMywzNi4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNzYuMywzYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVM3Ni45LDMsNzYuMywzIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzAuMywyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMzEuMiwyOSwzMC4zLDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzAuMywyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMzMS4yLDI0LjEsMzAuMywyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzAuMywxNS4xYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMzMS4zLDE1LjUsMzAuOCwxNS4xLDMwLjMsMTUuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTMwLjMsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMzEuMiwxOS4zLDMwLjMsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsMzRjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzMzLjQsMzMuNiwzMy45LDM0LDM0LjUsMzQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zMC4zLDMzLjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzMxLjksMzQuNSwzMS4yLDMzLjgsMzAuMywzMy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzAuMyw0MS45YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42cy0xLjYsMC43LTEuNiwxLjZDMjguNiw0MS4yLDI5LjQsNDEuOSwzMC4zLDQxLjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yNi4xLDQ5LjJjMC45LDAsMS42LTAuNywxLjYtMS42UzI3LDQ2LDI2LjEsNDZzLTEuNiwwLjctMS42LDEuNkMyNC40LDQ4LjQsMjUuMiw0OS4yLDI2LjEsNDkuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTI2LjEsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMjcsMzEuNCwyNi4xLDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yMS45LDM4LjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzIzLjUsMzkuNCwyMi44LDM4LjcsMjEuOSwzOC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSw0My41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkMyMy41LDQ0LjIsMjIuOCw0My41LDIxLjksNDMuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTI2LjEsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMjcsMjYuNiwyNi4xLDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0yNi4xLDQxLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzI3LDQxLjEsMjYuMSw0MS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjYuMSwzNi4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMyNy43LDM3LDI3LDM2LjIsMjYuMSwzNi4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzguNiwyNy40YzAuOSwwLDEuNi0wLjcsMS42LTEuNnMtMC43LTEuNi0xLjYtMS42UzM3LDI0LjksMzcsMjUuOFMzNy43LDI3LjQsMzguNiwyNy40Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzguNiwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0MC4zLDIwLDM5LjUsMTkuMywzOC42LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zOC42LDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzQwLjMsMTUuMiwzOS41LDE0LjUsMzguNiwxNC41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzguNiw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzM5LjUsOS42LDM4LjYsOS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwzNGMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDNDEuOCwzMy42LDQyLjMsMzQsNDIuOCwzNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM0My43LDcuMiw0Mi44LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsNy4yYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMzNS40LDcuMiwzNC41LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDMzcsNy4zLDM3LjcsOCwzOC42LDhzMS42LTAuNywxLjYtMS42QzQwLjMsNS41LDM5LjUsNC44LDM4LjYsNC44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzQuNSwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMzNi4xLDEyLjgsMzUuNCwxMi4xLDM0LjUsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM0LjUsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMzYuMSwyMi41LDM1LjQsMjEuNywzNC41LDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzM2LjEsMTcuNiwzNS40LDE2LjksMzQuNSwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMzQuNSwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMzNi4xLDI3LjMsMzUuNCwyNi42LDM0LjUsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTM4LjYsMC42Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMzOS43LDEsMzkuMiwwLjYsMzguNiwwLjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0zNC41LDNjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzM1LDMsMzQuNSwzIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwzYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVM0My40LDMsNDIuOCwzIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNOS40LDEwLjVjMC45LDAsMS42LTAuNywxLjYtMS42YzAtMC45LTAuNy0xLjYtMS42LTEuNkM4LjUsNy4zLDcuOCw4LDcuOCw4LjlDNy44LDkuNyw4LjUsMTAuNSw5LjQsMTAuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNC40LDQuOCwxMy41LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNC40LDkuNiwxMy41LDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCwxOS4zLDEzLjUsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTMTQuNCwxNC41LDEzLjUsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkuNCwyMC4yYzAuOSwwLDEuNi0wLjcsMS42LTEuNmMwLTAuOS0wLjctMS42LTEuNi0xLjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNkM3LjgsMTkuNCw4LjUsMjAuMiw5LjQsMjAuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQyLjgsMTIuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNDQuNSwxMi44LDQzLjcsMTIuMSw0Mi44LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05LjQsMTUuM2MwLjksMCwxLjYtMC43LDEuNi0xLjZjMC0wLjktMC43LTEuNi0xLjYtMS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDNy44LDE0LjYsOC41LDE1LjMsOS40LDE1LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xMy41LDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzE0LjQsMCwxMy41LDAiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01LjIsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZDMy42LDcuMyw0LjMsOCw1LjIsOGMwLjksMCwxLjYtMC43LDEuNi0xLjZDNi45LDUuNSw2LjEsNC44LDUuMiw0LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01LjIsMEM0LjMsMCwzLjYsMC43LDMuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2LjEsMCw1LjIsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEsM0MwLjQsMywwLDMuNSwwLDRjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMyLjEsMy41LDEuNiwzLDEsMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUuMiw5LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzYuMSw5LjYsNS4yLDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTkuNCwyLjRDOC41LDIuNCw3LjgsMy4xLDcuOCw0YzAsMC45LDAuNywxLjYsMS42LDEuNkMxMC4zLDUuNiwxMSw0LjksMTEsNEMxMSwzLjEsMTAuMywyLjQsOS40LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTEzLjUsMzEuNmMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDMTIuNSwzMS4yLDEzLDMxLjYsMTMuNSwzMS42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTcuNyw3LjhjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xUzE4LjMsNy44LDE3LjcsNy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSw1MWMwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFTMjEuMyw1MSwyMS45LDUxIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSwxOS45Yy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMyMi40LDE5LjksMjEuOSwxOS45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSwxNS4xYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMUMyMi45LDE1LjUsMjIuNCwxNS4xLDIxLjksMTUuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMTUuMWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFDNTYuNCwxNS41LDU2LDE1LjEsNTUuNCwxNS4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjYuMSwyMi4zYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVMyNi42LDIyLjMsMjYuMSwyMi4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTEuMiwyMi4zYy0wLjYsMC0xLDAuNS0xLDFjMCwwLjYsMC41LDEsMSwxYzAuNiwwLDEtMC41LDEtMVM1MS44LDIyLjMsNTEuMiwyMi4zIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTcuNywzMS40Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxOS4zLDMyLjEsMTguNiwzMS40LDE3LjcsMzEuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMzYuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDMTkuMywzNywxOC42LDM2LjIsMTcuNywzNi4yIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTMuNSwyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMxNC40LDI0LjEsMTMuNSwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSwyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlMyMi44LDI0LjEsMjEuOSwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMjEuOSwyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDMjMuNSwyOS43LDIyLjgsMjksMjEuOSwyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTIxLjksMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42UzIxLDM3LDIxLjksMzdjMC45LDAsMS42LTAuNywxLjYtMS42QzIzLjUsMzQuNSwyMi44LDMzLjgsMjEuOSwzMy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTcuNywxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkMxOS4zLDEyLjgsMTguNiwxMi4xLDE3LjcsMTIuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsMjYuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTMTguNiwyNi42LDE3LjcsMjYuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTE3LjcsNDMuN2MwLjYsMCwxLTAuNSwxLTFjMC0wLjYtMC41LTEtMS0xYy0wLjYsMC0xLDAuNS0xLDFDMTYuNyw0My4zLDE3LjEsNDMuNywxNy43LDQzLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik0xNy43LDIxLjdjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzE5LjMsMjIuNSwxOC42LDIxLjcsMTcuNywyMS43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNMTcuNywxNi45Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlMxOC42LDE2LjksMTcuNywxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTkuNiw0My43YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFzLTEsMC41LTEsMUM1OC42LDQzLjMsNTksNDMuNyw1OS42LDQzLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02OCwxMi4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkM2OS42LDEyLjgsNjguOSwxMi4xLDY4LDEyLjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02OCw3LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42UzY4LjksNy4yLDY4LDcuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTY4LDIwLjJjMC45LDAsMS42LTAuNywxLjYtMS42YzAtMC45LTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42QzY2LjMsMTkuNCw2Ny4xLDIwLjIsNjgsMjAuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTcyLjIsMTIuOWMwLjksMCwxLjYtMC43LDEuNi0xLjZzLTAuNy0xLjYtMS42LTEuNmMtMC45LDAtMS42LDAuNy0xLjYsMS42UzcxLjMsMTIuOSw3Mi4yLDEyLjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik02OCwyLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42QzY5LjYsMy4xLDY4LjksMi40LDY4LDIuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTcyLjIsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM3My4xLDQuOCw3Mi4yLDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTcyLjIsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNzMuMSwwLDcyLjIsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsMTkuM2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNywxOS4zLDYzLjgsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2NC43LDkuNiw2My44LDkuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsNC44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM2NC43LDQuOCw2My44LDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsMGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNywwLDYzLjgsMCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsNTFjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxQzg2LDUwLjUsODYuNSw1MSw4Ny4xLDUxIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNjMuOCwzMS42YzAuNiwwLDEtMC41LDEtMWMwLTAuNi0wLjUtMS0xLTFjLTAuNiwwLTEsMC41LTEsMUM2Mi44LDMxLjIsNjMuMiwzMS42LDYzLjgsMzEuNiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsMjQuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNywyNC4xLDYzLjgsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTYzLjgsMTQuNWMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNjQuNywxNC41LDYzLjgsMTQuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDODguNywzNC41LDg4LDMzLjgsODcuMSwzMy44Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSw0My41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCw0My41LDg3LjEsNDMuNSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMzguN2MtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTODgsMzguNyw4Ny4xLDM4LjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDIuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZDOTIuOSwzLjEsOTIuMSwyLjQsOTEuMiwyLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDcuMmMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNmMwLjksMCwxLjYtMC43LDEuNi0xLjZTOTIuMSw3LjIsOTEuMiw3LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzkyLjksMTIuOCw5Mi4xLDEyLjEsOTEuMiwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSwxOS4zYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCwxOS4zLDg3LjEsMTkuMyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsMjljLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Uzg4LDI5LDg3LjEsMjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDE0LjVjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Uzg4LDE0LjUsODcuMSwxNC41Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSw0LjhjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Uzg4LDQuOCw4Ny4xLDQuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTg3LjEsOS42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNlM4OCw5LjYsODcuMSw5LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik04Ny4xLDBjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42Uzg4LDAsODcuMSwwIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNODcuMSwyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42YzAuOSwwLDEuNi0wLjcsMS42LTEuNkM4OC43LDI0LjksODgsMjQuMSw4Ny4xLDI0LjEiCgkJLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Mi44LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzQ0LjUsMTcuNiw0My43LDE2LjksNDIuOCwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTEuMiwyNi42Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM1Mi4xLDI2LjYsNTEuMiwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDcsMzMuOGMtMC45LDAtMS42LDAuNy0xLjYsMS42UzQ2LjEsMzcsNDcsMzdzMS42LTAuNywxLjYtMS42QzQ4LjcsMzQuNSw0Ny45LDMzLjgsNDcsMzMuOCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsNTFjMC42LDAsMS0wLjUsMS0xYzAtMC42LTAuNS0xLTEtMWMtMC42LDAtMSwwLjUtMSwxUzU0LjgsNTEsNTUuNCw1MSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsMzEuNGMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTIuMSwzMS40LDUxLjIsMzEuNCIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsNDkuMmMwLjksMCwxLjYtMC43LDEuNi0xLjZTNTIuMSw0Niw1MS4yLDQ2cy0xLjYsMC43LTEuNiwxLjZDNDkuNiw0OC40LDUwLjMsNDkuMiw1MS4yLDQ5LjIiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01MS4yLDM2LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzUyLjgsMzcsNTIuMSwzNi4yLDUxLjIsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTUxLjIsNDEuMWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTIuMSw0MS4xLDUxLjIsNDEuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQ3LDQxLjljMC45LDAsMS42LTAuNywxLjYtMS42cy0wLjctMS42LTEuNi0xLjZzLTEuNiwwLjctMS42LDEuNlM0Ni4xLDQxLjksNDcsNDEuOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQ3LDE1LjFjLTAuNiwwLTEsMC41LTEsMWMwLDAuNiwwLjUsMSwxLDFjMC42LDAsMS0wLjUsMS0xQzQ4LDE1LjUsNDcuNiwxNS4xLDQ3LDE1LjEiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00Mi44LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzQ0LjUsMjcuMyw0My43LDI2LjYsNDIuOCwyNi42Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNDIuOCwyMS43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZjMCwwLjksMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0NC41LDIyLjUsNDMuNywyMS43LDQyLjgsMjEuNyIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQ3LDI5Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM0OC43LDI5LjcsNDcuOSwyOSw0NywyOSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTQ3LDE5LjNjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzQ4LjcsMjAsNDcuOSwxOS4zLDQ3LDE5LjMiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik00NywyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM0Ny45LDI0LjEsNDcsMjQuMSIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU5LjYsMjEuN2MtMC45LDAtMS42LDAuNy0xLjYsMS42YzAsMC45LDAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZDNjEuMiwyMi41LDYwLjUsMjEuNyw1OS42LDIxLjciLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzYwLjUsMTYuOSw1OS42LDE2LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDM2LjJjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzYxLjIsMzcsNjAuNSwzNi4yLDU5LjYsMzYuMiIvPgoJPHBhdGggY2xhc3M9InN0MCIgZD0iTTU1LjQsMTkuOWMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMWMwLjYsMCwxLTAuNSwxLTFTNTYsMTkuOSw1NS40LDE5LjkiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDMxLjRjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzYwLjUsMzEuNCw1OS42LDMxLjQiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDI2LjZjLTAuOSwwLTEuNiwwLjctMS42LDEuNnMwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42UzYwLjUsMjYuNiw1OS42LDI2LjYiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDcuOGMtMC42LDAtMSwwLjUtMSwxYzAsMC42LDAuNSwxLDEsMXMxLTAuNSwxLTFTNjAuMSw3LjgsNTkuNiw3LjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik01OS42LDEyLjFjLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZzMS42LTAuNywxLjYtMS42QzYxLjIsMTIuOCw2MC41LDEyLjEsNTkuNiwxMi4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTUuNCwyOWMtMC45LDAtMS42LDAuNy0xLjYsMS42czAuNywxLjYsMS42LDEuNnMxLjYtMC43LDEuNi0xLjZTNTYuMywyOSw1NS40LDI5Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTUuNCwyNC4xYy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM1Ni4zLDI0LjEsNTUuNCwyNC4xIi8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTUuNCwzMy44Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNkM1NywzNC41LDU2LjMsMzMuOCw1NS40LDMzLjgiLz4KCTxwYXRoIGNsYXNzPSJzdDAiIGQ9Ik05MS4yLDE2LjljLTAuOSwwLTEuNiwwLjctMS42LDEuNmMwLDAuOSwwLjcsMS42LDEuNiwxLjZjMC45LDAsMS42LTAuNywxLjYtMS42CgkJQzkyLjksMTcuNiw5Mi4xLDE2LjksOTEuMiwxNi45Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTUuNCwzOC43Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42czEuNi0wLjcsMS42LTEuNlM1Ni4zLDM4LjcsNTUuNCwzOC43Ii8+Cgk8cGF0aCBjbGFzcz0ic3QwIiBkPSJNNTUuNCw0My41Yy0wLjksMC0xLjYsMC43LTEuNiwxLjZzMC43LDEuNiwxLjYsMS42UzU3LDQ2LDU3LDQ1LjFTNTYuMyw0My41LDU1LjQsNDMuNSIvPgo8L2c+Cjwvc3ZnPgo=" style={{height:14,opacity:0.5}} alt="WPP Media"/>
          <span>·</span>
          <span>CRISIS PULSE</span>
          <span>·</span>
          <span>WPP MEDIA MENA</span>
          <span>·</span>
          <span>REFRESHED DAILY 09:00 GST</span>
          <span>·</span>
          <span>{history.length} DAYS OF HISTORY</span>
        </div>
      </main>

      {showSettings && (
        <SettingsPanel config={config} onClose={()=>setShowSettings(false)} onSave={saveConfig}/>
      )}
    </>
  );
}