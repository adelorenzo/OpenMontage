"""OpenMontage remote API server.

A thin MCP (control plane) + HTTP (data plane) shell that lets a remote,
MCP-native agent delegate full video-production jobs to a headless OpenMontage
agent running on this (GPU) machine. See docs/REMOTE_API.md for the design.

The intelligence stays in the skills/manifests; this package only accepts jobs,
runs the headless agent, tracks state, and serves the resulting artifacts.
"""
