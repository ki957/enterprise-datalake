// Matte Plotly theme — matches the deep navy / smoky cyan palette
// Import and spread into every Plot layout prop

export const PLOTLY_LAYOUT = {
  paper_bgcolor: 'transparent',
  plot_bgcolor:  'transparent',
  font: {
    family: "'JetBrains Mono', 'Fira Code', monospace",
    size:   11,
    color:  '#8899AA',
  },
  title: {
    font:  { size: 13, color: '#E8EAF0', family: "'Inter', sans-serif" },
    x:     0.02,
    pad:   { t: 4, b: 8 },
  },
  margin: { t: 40, r: 12, b: 40, l: 44 },
  xaxis: {
    gridcolor:      '#1E2A45',
    linecolor:      '#1E2A45',
    tickcolor:      '#1E2A45',
    zerolinecolor:  '#1E2A45',
    tickfont:       { size: 10, color: '#8899AA' },
  },
  yaxis: {
    gridcolor:      '#1E2A45',
    linecolor:      '#1E2A45',
    tickcolor:      '#1E2A45',
    zerolinecolor:  '#1E2A45',
    tickfont:       { size: 10, color: '#8899AA' },
  },
  legend: {
    bgcolor:     'rgba(15,22,41,0.6)',
    bordercolor: '#1E2A45',
    borderwidth: 1,
    font:        { size: 11, color: '#8899AA' },
  },
  hoverlabel: {
    bgcolor:     '#141E35',
    bordercolor: '#1E2A45',
    font:        { size: 11, color: '#E8EAF0', family: "'Inter', sans-serif" },
  },
  colorway: [
    '#5BC8D4',  // cyan
    '#7C6FA0',  // violet
    '#10B981',  // emerald
    '#3B82F6',  // blue
    '#F59E0B',  // amber
    '#EF4444',  // red
    '#6366F1',  // indigo
    '#8B5CF6',  // purple
  ],
}

export const PLOTLY_CONFIG = {
  displayModeBar:  false,
  responsive:      true,
  staticPlot:      false,
}
