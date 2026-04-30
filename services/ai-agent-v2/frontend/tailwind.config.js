/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      screens: {
        // Aligned to target devices
        // default  → 0px+    mobile (360-430dp)
        // md       → 768px+  tablet portrait
        // lg       → 1024px+ tablet landscape
        // xl       → 1280px+ Samsung Tab S9 FE+ / desktop
        '2xl': '1440px',
      },
      colors: {
        base:     '#0A0F1E',
        surface:  '#0F1629',
        elevated: '#141E35',
        border:   '#1E2A45',
        cyan: {
          DEFAULT: '#5BC8D4',
          text:    '#7DD4DC',
          muted:   '#2D6B73',
          dim:     '#0D2D32',
        },
        violet: {
          DEFAULT: '#7C6FA0',
          text:    '#9B8FC4',
          muted:   '#3D3560',
          dim:     '#1A1730',
        },
        ink: {
          primary:   '#E8EAF0',
          secondary: '#8899AA',
          muted:     '#3A4A5C',
        },
        agent: {
          insight:       '#5BC8D4',
          quality:       '#10B981',
          ingestion:     '#3B82F6',
          orchestration: '#F59E0B',
          performance:   '#EF4444',
          schema:        '#7C6FA0',
          airbyte:       '#6366F1',
          auto:          '#8899AA',
          error:         '#EF4444',
        },
        status: {
          up:      '#10B981',
          down:    '#EF4444',
          unknown: '#8899AA',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      fontSize: {
        '3xs': ['9px',  '12px'],
        '2xs': ['10px', '14px'],
        xs:    ['11px', '16px'],
        sm:    ['13px', '20px'],
        base:  ['14px', '22px'],
        md:    ['15px', '24px'],
        lg:    ['16px', '26px'],
        xl:    ['18px', '28px'],
        '2xl': ['20px', '30px'],
        '3xl': ['24px', '32px'],
      },
      borderRadius: {
        sm:  '6px',
        DEFAULT: '8px',
        md:  '10px',
        lg:  '14px',
        xl:  '18px',
        '2xl': '24px',
        full: '9999px',
      },
      boxShadow: {
        'glass':  '0 4px 24px 0 rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.04)',
        'card':   '0 2px 12px 0 rgba(0,0,0,0.3)',
        'input':  '0 0 0 1px rgba(91,200,212,0.2)',
        'input-focus': '0 0 0 2px rgba(91,200,212,0.35)',
      },
      backdropBlur: {
        xs: '4px',
        sm: '8px',
        DEFAULT: '16px',
        lg: '24px',
      },
      animation: {
        'pulse-slow':  'pulse 3s cubic-bezier(0.4,0,0.6,1) infinite',
        'fade-in':     'fadeIn 0.2s ease-out',
        'slide-up':    'slideUp 0.25s ease-out',
        'slide-right': 'slideRight 0.25s ease-out',
        'blink':       'blink 1.2s step-start infinite',
      },
      keyframes: {
        fadeIn: {
          '0%':   { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%':   { opacity: '0', transform: 'translateY(8px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        slideRight: {
          '0%':   { opacity: '0', transform: 'translateX(-12px)' },
          '100%': { opacity: '1', transform: 'translateX(0)' },
        },
        blink: {
          '0%, 100%': { opacity: '1' },
          '50%':      { opacity: '0' },
        },
      },
    },
  },
  plugins: [],
}
