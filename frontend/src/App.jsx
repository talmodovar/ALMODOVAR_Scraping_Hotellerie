import React, { useState, useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import {
  setSearchQuery,
  fetchSearchStart,
  fetchSearchSuccess,
  fetchSearchFailure,
  fetchProfileStart,
  fetchProfileSuccess,
  fetchProfileFailure,
  fetchDirectorsStart,
  fetchDirectorsSuccess,
  fetchDirectorsFailure,
  clearActiveProfile,
  startStatutesStream,
  addStatuteEvent,
  stopStatutesStream
} from './store';
import {
  Search,
  Building,
  Briefcase,
  Users,
  TrendingUp,
  FileText,
  Loader,
  Calendar,
  DollarSign
} from 'lucide-react';
import {
  Sankey,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend
} from 'recharts';
import './App.css';

const API_BASE = 'http://localhost:8000';

function App() {
  const dispatch = useDispatch();
  const {
    searchQuery,
    searchResults,
    searchLoading,
    activeEnterprise,
    profileLoading,
    directors,
    directorsLoading,
    statutes,
    statutesStreaming,
    statutesError
  } = useSelector((state) => state.bce);

  const [showDropdown, setShowDropdown] = useState(false);
  const [selectedYear, setSelectedYear] = useState(null);
  const dropdownRef = useRef(null);

  // Close search dropdown on click outside
  useEffect(() => {
    function handleClickOutside(event) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Debounced search query listener
  useEffect(() => {
    if (searchQuery.trim().length < 2) {
      dispatch(fetchSearchSuccess([]));
      return;
    }

    const timer = setTimeout(async () => {
      dispatch(fetchSearchStart());
      try {
        const res = await fetch(`${API_BASE}/api/enterprises/search?q=${encodeURIComponent(searchQuery)}`);
        if (res.ok) {
          const data = await res.json();
          dispatch(fetchSearchSuccess(data));
        } else {
          dispatch(fetchSearchFailure('Failed to fetch search results'));
        }
      } catch (err) {
        dispatch(fetchSearchFailure(err.message));
      }
    }, 300);

    return () => clearTimeout(timer);
  }, [searchQuery, dispatch]);

  // Reset selected year when company profile changes
  useEffect(() => {
    if (activeEnterprise?.gold_data?.years?.length > 0) {
      // Set to the latest available year
      const years = [...activeEnterprise.gold_data.years].sort((a, b) => b.year - a.year);
      setSelectedYear(years[0].year);
    } else {
      setSelectedYear(null);
    }
  }, [activeEnterprise]);

  // Select search result
  const handleSelectEnterprise = async (entNum) => {
    setShowDropdown(false);
    dispatch(fetchProfileStart());
    try {
      // 1. Fetch general info + ratios
      const res = await fetch(`${API_BASE}/api/enterprises/${entNum}`);
      if (!res.ok) throw new Error('Company not found');
      const data = await res.json();
      dispatch(fetchProfileSuccess(data));

      // 2. Fetch directors from kbopub
      dispatch(fetchDirectorsStart());
      const dirRes = await fetch(`${API_BASE}/api/enterprises/${entNum}/directors`);
      if (dirRes.ok) {
        const dirData = await dirRes.json();
        dispatch(fetchDirectorsSuccess(dirData));
      } else {
        dispatch(fetchDirectorsFailure('Failed to fetch directors'));
      }
    } catch (err) {
      dispatch(fetchProfileFailure(err.message));
    }
  };

  // Launch statutes stream
  const handleLoadStatutes = () => {
    if (!activeEnterprise) return;
    const entNum = activeEnterprise.EnterpriseNumber;
    const cleaned = entNum.replace(/\./g, '').replace(/ /g, '');

    dispatch(startStatutesStream());

    const eventSource = new EventSource(`${API_BASE}/api/enterprises/${cleaned}/statutes/stream`);

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        dispatch(addStatuteEvent(data));
        if (data.event === 'done' || data.event === 'error') {
          eventSource.close();
        }
      } catch (err) {
        console.error('Failed to parse SSE line:', err);
      }
    };

    eventSource.onerror = (err) => {
      console.error('SSE connection error:', err);
      dispatch(addStatuteEvent({ event: 'error', message: 'Connection to streaming scraper lost.' }));
      eventSource.close();
    };
  };

  // Get primary denomination name
  const getCompanyName = (ent) => {
    if (!ent) return '';
    if (ent.denominations?.length > 0) {
      return ent.denominations[0].Denomination;
    }
    return 'Entreprise sans dénomination';
  };

  // Get main address string
  const getAddress = (ent) => {
    if (!ent || !ent.addresses?.length) return 'Adresse non renseignée';
    const addr = ent.addresses[0];
    const street = addr.StreetFR || addr.StreetNL || '';
    const num = addr.HouseNumber || '';
    const zip = addr.Zipcode || '';
    const city = addr.MunicipalityFR || addr.MunicipalityNL || '';
    return `${street} ${num}, ${zip} ${city}`.trim();
  };

  // Calculate Sankey data
  const getSankeyData = () => {
    if (!activeEnterprise?.gold_data?.years || !selectedYear) return null;
    const yearData = activeEnterprise.gold_data.years.find(y => y.year === selectedYear);
    if (!yearData) return null;

    const ca = Math.max(0, yearData.ca || 0);
    const mb = Math.max(0, yearData.marge_brute || 0);
    const rn = Math.max(0, yearData.resultat_net || 0);

    return {
      nodes: [
        { name: `Chiffre d'Affaires (${ca.toLocaleString()} €)` },
        { name: `Marge Brute (${mb.toLocaleString()} €)` },
        { name: `Résultat Net (${rn.toLocaleString()} €)` }
      ],
      links: [
        { source: 0, target: 1, value: Math.max(1, mb) },
        { source: 1, target: 2, value: Math.max(1, rn) }
      ]
    };
  };

  const sankeyData = getSankeyData();

  // Format ratio list for charts
  const getChartData = () => {
    if (!activeEnterprise?.gold_data?.years) return [];
    return [...activeEnterprise.gold_data.years]
      .sort((a, b) => a.year - b.year)
      .map(y => ({
        year: y.year,
        'ROE (%)': y.ratios?.roe ? parseFloat(y.ratios.roe.toFixed(2)) : 0,
        'Marge Brute (%)': y.ratios?.marge_brute ? parseFloat(y.ratios.marge_brute.toFixed(2)) : 0,
        'Marge Nette (%)': y.ratios?.marge_nette ? parseFloat(y.ratios.marge_nette.toFixed(2)) : 0,
        'Endettement (%)': y.ratios?.endettement ? parseFloat(y.ratios.endettement.toFixed(2)) : 0,
        'Liquidité': y.ratios?.liquidite ? parseFloat(y.ratios.liquidite.toFixed(2)) : 0
      }));
  };

  const chartData = getChartData();

  return (
    <div className="app-container">
      <header>
        <h1>Belgian Enterprise Dashboard</h1>
        <p>Rechercher des entreprises hôtelières belges et visualiser leurs données Gold & Silver</p>
      </header>

      {/* SEARCH SECTION */}
      <div className="search-section" ref={dropdownRef}>
        <div className="search-input-wrapper">
          <Search className="search-icon" />
          <input
            type="text"
            className="search-input"
            placeholder="Rechercher par nom ou numéro BCE (ex: Apple, 0836157420)..."
            value={searchQuery}
            onChange={(e) => {
              dispatch(setSearchQuery(e.target.value));
              setShowDropdown(true);
            }}
            onFocus={() => setShowDropdown(true)}
          />
          {searchLoading && (
            <Loader className="search-icon animate-spin" style={{ left: 'auto', right: '1.25rem' }} />
          )}
        </div>

        {showDropdown && searchResults.length > 0 && (
          <div className="search-results-dropdown">
            {searchResults.map((ent) => (
              <div
                key={ent.EnterpriseNumber}
                className="result-item"
                onClick={() => handleSelectEnterprise(ent.EnterpriseNumber)}
              >
                <div className="result-info">
                  <span className="result-name">{getCompanyName(ent)}</span>
                  <span className="result-bce">{ent.EnterpriseNumber}</span>
                </div>
                <div className="result-meta">
                  <span className={`badge ${ent.StatusLabel === 'Actif' ? 'active' : ''}`}>
                    {ent.StatusLabel || 'Inconnu'}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* DASHBOARD DETAILS */}
      {profileLoading ? (
        <div className="empty-state" style={{ borderStyle: 'none' }}>
          <Loader className="animate-spin" style={{ width: '3rem', height: '3rem', color: '#5b73e8', margin: '0 auto 1.5rem auto' }} />
          <h3>Chargement de la fiche entreprise...</h3>
          <p>Récupération des données Silver et Gold</p>
        </div>
      ) : activeEnterprise ? (
        <div className="dashboard-grid">
          
          {/* LEFT COLUMN: SILVER PROFILE & DIRECTORS */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
            
            {/* General Card */}
            <div className="card">
              <h2><Building className="card-icon" /> Profil Entreprise</h2>
              <div className="company-header">
                <h3 className="company-title">{getCompanyName(activeEnterprise)}</h3>
                <div className="company-sub">BCE: {activeEnterprise.EnterpriseNumber}</div>
              </div>

              <div className="info-grid">
                <div className="info-item">
                  <span className="info-label">Statut</span>
                  <span className="info-val">{activeEnterprise.StatusLabel || 'Actif'}</span>
                </div>
                <div className="info-item">
                  <span className="info-label">Forme Juridique</span>
                  <span className="info-val">{activeEnterprise.JuridicalFormLabel || 'Société'}</span>
                </div>
                <div className="info-item">
                  <span className="info-label">Siège Social</span>
                  <span className="info-val">{getAddress(activeEnterprise)}</span>
                </div>
                <div className="info-item">
                  <span className="info-label">Date de Création</span>
                  <span className="info-val">{activeEnterprise.StartDate || 'N/C'}</span>
                </div>
              </div>
            </div>

            {/* Activities Card */}
            <div className="card">
              <h2><Briefcase className="card-icon" /> Activités NACE</h2>
              <div className="activities-list">
                {activeEnterprise.activities?.length > 0 ? (
                  activeEnterprise.activities.map((act, i) => (
                    <div key={i} className="activity-badge">
                      <span className="activity-code">{act.NaceCode}</span>
                      <span>—</span>
                      <span className="activity-label">{act.NaceLabel}</span>
                    </div>
                  ))
                ) : (
                  <p style={{ color: 'var(--text-light)', margin: 0 }}>Aucune activité répertoriée</p>
                )}
              </div>
            </div>

            {/* Directors Card */}
            <div className="card">
              <h2><Users className="card-icon" /> Dirigeants & Représentants</h2>
              {directorsLoading ? (
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--text-light)' }}>
                  <Loader className="animate-spin" style={{ width: '1.25rem', height: '1.25rem' }} />
                  <span>Récupération depuis kbopub...</span>
                </div>
              ) : directors.length > 0 ? (
                <div className="directors-list">
                  {directors.map((dir, i) => (
                    <div key={i} className="director-item">
                      <div>
                        <div className="director-name">{dir.name}</div>
                        <div className="director-role">{dir.role}</div>
                      </div>
                      {dir.start_date && (
                        <div className="director-since">{dir.start_date}</div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <p style={{ color: 'var(--text-light)', margin: 0 }}>Aucun dirigeant répertorié</p>
              )}
            </div>
          </div>

          {/* RIGHT COLUMN: FINANCIAL RATIOS (GOLD) & SANKEY & NOTARY ACTS */}
          <div className="financials-section">
            
            {/* Ratios & Chart Card */}
            <div className="card">
              <h2><TrendingUp className="card-icon" /> Ratios Financiers (Gold)</h2>
              
              {activeEnterprise.gold_data?.years?.length > 0 ? (
                <div>
                  <div className="table-wrapper" style={{ marginBottom: '1.5rem' }}>
                    <table className="ratios-table">
                      <thead>
                        <tr>
                          <th>Année</th>
                          <th>ROE</th>
                          <th>Marge Brute</th>
                          <th>Marge Nette</th>
                          <th>Endettement</th>
                          <th>Liquidité</th>
                        </tr>
                      </thead>
                      <tbody>
                        {[...activeEnterprise.gold_data.years]
                          .sort((a, b) => b.year - a.year)
                          .map((yr, idx) => (
                            <tr key={idx}>
                              <td style={{ fontWeight: 600 }}>{yr.year}</td>
                              <td className="ratio-value">{yr.ratios?.roe ? `${yr.ratios.roe.toFixed(1)} %` : 'N/C'}</td>
                              <td className="ratio-value">{yr.ratios?.marge_brute ? `${yr.ratios.marge_brute.toFixed(1)} %` : 'N/C'}</td>
                              <td className="ratio-value">{yr.ratios?.marge_nette ? `${yr.ratios.marge_nette.toFixed(1)} %` : 'N/C'}</td>
                              <td className="ratio-value">{yr.ratios?.endettement ? `${yr.ratios.endettement.toFixed(1)} %` : 'N/C'}</td>
                              <td className="ratio-value">{yr.ratios?.liquidite ? yr.ratios.liquidite.toFixed(2) : 'N/C'}</td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>

                  <h3 style={{ fontSize: '1rem', fontWeight: 600, margin: '1rem 0' }}>Évolution des Ratios Clés</h3>
                  <div style={{ width: '100%', height: 220 }}>
                    <ResponsiveContainer>
                      <LineChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis dataKey="year" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Line type="monotone" dataKey="ROE (%)" stroke="#b3c5ff" strokeWidth={3} activeDot={{ r: 8 }} />
                        <Line type="monotone" dataKey="Marge Brute (%)" stroke="#ffd5dc" strokeWidth={3} />
                        <Line type="monotone" dataKey="Marge Nette (%)" stroke="#ffcad4" strokeWidth={3} />
                        <Line type="monotone" dataKey="Endettement (%)" stroke="#e2d4ff" strokeWidth={3} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              ) : (
                <div style={{ textAlign: 'center', padding: '1rem', color: 'var(--text-light)' }}>
                  Aucun bilan financier disponible dans la base Gold pour calculer les ratios.
                </div>
              )}
            </div>

            {/* P&L Sankey Flow Card */}
            {activeEnterprise.gold_data?.years?.length > 0 && (
              <div className="card">
                <div className="financials-header">
                  <h2><DollarSign className="card-icon" /> Sankey Compte de Résultats</h2>
                  <select
                    className="year-select"
                    value={selectedYear || ''}
                    onChange={(e) => setSelectedYear(parseInt(e.target.value))}
                  >
                    {[...activeEnterprise.gold_data.years]
                      .sort((a, b) => b.year - a.year)
                      .map((yr) => (
                        <option key={yr.year} value={yr.year}>{yr.year}</option>
                      ))}
                  </select>
                </div>

                {sankeyData ? (
                  <div className="sankey-container">
                    <ResponsiveContainer width="100%" height={220}>
                      <Sankey
                        data={sankeyData}
                        node={{ fill: '#b3c5ff', stroke: '#5b73e8', strokeWidth: 1 }}
                        link={{ stroke: '#ffd5dc' }}
                      >
                        <Tooltip />
                      </Sankey>
                    </ResponsiveContainer>
                  </div>
                ) : (
                  <p style={{ color: 'var(--text-light)', margin: 0 }}>Aucune donnée comptable pour cette année.</p>
                )}
              </div>
            )}

            {/* Detailed Financial Statements Card */}
            {activeEnterprise.gold_data?.years?.length > 0 && selectedYear && (
              <div className="card">
                <h2><DollarSign className="card-icon" /> États Financiers ({selectedYear})</h2>
                {(() => {
                  const yrData = activeEnterprise.gold_data.years.find(y => y.year === selectedYear);
                  if (!yrData) return <p style={{ color: 'var(--text-light)', margin: 0 }}>Aucune donnée disponible</p>;
                  return (
                    <div className="info-grid">
                      <div className="info-item">
                        <span className="info-label">Chiffre d'Affaires</span>
                        <span className="info-val">{yrData.ca !== undefined && yrData.ca !== null ? `${yrData.ca.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Marge Brute (Montant)</span>
                        <span className="info-val">{yrData.marge_brute !== undefined && yrData.marge_brute !== null ? `${yrData.marge_brute.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">EBIT (Opérationnel)</span>
                        <span className="info-val">{yrData.ebit !== undefined && yrData.ebit !== null ? `${yrData.ebit.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Résultat Net</span>
                        <span className="info-val">{yrData.resultat_net !== undefined && yrData.resultat_net !== null ? `${yrData.resultat_net.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Trésorerie</span>
                        <span className="info-val">{yrData.tresorerie !== undefined && yrData.tresorerie !== null ? `${yrData.tresorerie.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Dettes Financières</span>
                        <span className="info-val">{yrData.dettes_financieres !== undefined && yrData.dettes_financieres !== null ? `${yrData.dettes_financieres.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Fonds Propres</span>
                        <span className="info-val">{yrData.fonds_propres !== undefined && yrData.fonds_propres !== null ? `${yrData.fonds_propres.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                      <div className="info-item">
                        <span className="info-label">Capital Souscrit</span>
                        <span className="info-val">{yrData.capital_souscrit !== undefined && yrData.capital_souscrit !== null ? `${yrData.capital_souscrit.toLocaleString()} €` : 'N/C'}</span>
                      </div>
                    </div>
                  );
                })()}
              </div>
            )}


          </div>
        </div>
      ) : (
        <div className="empty-state">
          <Building style={{ width: '4rem', height: '4rem', color: 'var(--pastel-blue)', margin: '0 auto 1.5rem auto' }} />
          <h3>Aucune entreprise sélectionnée</h3>
          <p>Utilisez la barre de recherche ci-dessus pour sélectionner une entreprise.</p>
        </div>
      )}
    </div>
  );
}

export default App;
