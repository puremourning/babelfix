def Settings(**kwargs):
  if kwargs['language'] == 'rust':
    return {
      'ls': {
        'check': {
          'command': 'clippy',
        },
        'cargo': {
          'features': 'all',
          # 'extraArgs': [ '-Z', 'bindeps' ],
        },
        'rustfmt': {
          'rangeFormatting': {
            'enable': True,
          },
        },
        'diagnostics': {
          'disabled': [
            'inactive-code',
          ]
        },
        'inlayHints': {
          'bindingModeHints': { 'enable': True },
          'closureCaptureHints': { 'enable': True },
          'closureReturnTypeHints': { 'enable': True },
          'genericParameterHints': {
            'lifetime': { 'enable': True },
            'type': { 'enable': True },
          }
        },
        'workspace': {
          'symbol': {
            'search': {
              'kind': 'all_symbols',
              'scope': 'workspace',
              'limit': 1024,
            },
          },
        },
      },
    }
