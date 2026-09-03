#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <version> <arm64_sha256> <x86_64_sha256>" >&2
  exit 1
fi

version="$1"
arm_sha="$2"
intel_sha="$3"

cat <<EOF
class AgentOrchestrator < Formula
  desc "Deterministic SQ-driven supervisor for local coding agents"
  homepage "https://github.com/DerekStride/agent-orchestrator"
  version "${version}"
  license "MIT"

  on_arm do
    url "https://github.com/DerekStride/agent-orchestrator/releases/download/v#{version}/agent-orchestrator-v#{version}-aarch64-apple-darwin.tar.gz"
    sha256 "${arm_sha}"
  end

  on_intel do
    url "https://github.com/DerekStride/agent-orchestrator/releases/download/v#{version}/agent-orchestrator-v#{version}-x86_64-apple-darwin.tar.gz"
    sha256 "${intel_sha}"
  end

  def install
    bin.install "agent-orchestrator"
  end

  test do
    output = shell_output("#{bin}/agent-orchestrator prime")
    assert_match "# agent-orchestrator", output
  end
end
EOF
