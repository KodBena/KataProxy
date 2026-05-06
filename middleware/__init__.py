"""
middleware — Layer 1 session-middleware extensions for the proxy pipeline.

A ``SessionMiddleware`` (ABC defined in ``middleware.session_middleware``)
is an async generator over the response stream, instantiated per
``ClientSession``. Stateful. Can buffer, suppress, fan out, or inject
follow-up queries via ``submit_query``. Reach for middleware when the
work needs cross-message state, async awaits, or control over *when*
responses are emitted — not just *what* they contain.

Modules in this package:

  - ``session_middleware``  — the ``SessionMiddleware`` ABC,
                              ``MiddlewareChain``, ``IdentityMiddleware``,
                              ``SessionCapabilities`` (the lifetime-of-
                              session callback bundle including
                              ``submit_query`` / ``terminate_query``),
                              and the public type aliases
                              (``SubmitQuery``, ``TerminateQuery``,
                              ``ResponseStream``).
  - ``keep_alive``          — ``KeepAliveMiddleware``, the per-session
                              inactivity watchdog (catches the
                              WS-stays-open-but-silent case that
                              disconnect-side cleanup cannot).
  - ``adaptive_reevaluate`` — ``AdaptiveReevaluateMiddleware``, the
                              "all three at once" worked example —
                              cross-message state, async submission of
                              follow-up deeper queries, control over
                              when finals are emitted.

The transformer extension surface lives next door in ``transformers/``.
See ``ARCHITECTURE.md`` for the choice criteria.
"""
