"use client";

import Link from "next/link";
import Image from "next/image";
import { m } from "framer-motion";
import { Snowflake, ArrowRight, ArrowLeft, Check, Zap, Shield, Code2 } from "lucide-react";
import { Header } from "@/components/header";
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar";
import { SessionSidebar } from "@/components/session-sidebar";

const features = [
  {
    icon: Zap,
    label: "Automated Conversion",
    desc: "AI-powered SQL transformation across 15+ source databases",
  },
  {
    icon: Shield,
    label: "Validation Engine",
    desc: "Syntax and semantic checks before deployment",
  },
  {
    icon: Code2,
    label: "Multi-Format Support",
    desc: "Stored procedures, UDFs, views, and DDL scripts",
  },
];

const steps = [
  "Choose source database and content type",
  "Upload source SQL files",
  "Provide Snowflake connection details",
  "Review and start migration",
];

export default function MigrationToolkitPage() {
  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#1a1a1a]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header />

      <SidebarProvider className="sidebar-offset min-h-0 flex-1">
        <div className="flex min-h-0 w-full flex-1">
          <SessionSidebar />

          <SidebarInset className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#07080c]">
            <div className="relative flex min-h-0 flex-1 items-center justify-center overflow-hidden px-4 pb-10 pt-8 sm:px-6">
              {/* Background image */}
              <div className="pointer-events-none absolute inset-0 z-0 bg-[url('/migration-toolkit-bg.png')] bg-cover bg-center opacity-95 saturate-[1.2]" />
              {/* Gradient overlays */}
              <div className="pointer-events-none absolute inset-0 z-0 bg-[linear-gradient(90deg,rgba(7,8,12,0.34)_0%,rgba(7,8,12,0.62)_42%,rgba(7,8,12,0.9)_100%)]" />
              <div className="pointer-events-none absolute inset-0 z-0 bg-[radial-gradient(circle_at_24%_28%,rgba(41,181,232,0.18),transparent_42%),radial-gradient(circle_at_72%_16%,rgba(20,136,252,0.22),transparent_45%),radial-gradient(circle_at_86%_72%,rgba(255,255,255,0.08),transparent_30%)]" />
              {/* Noise texture */}
              <div className="bg-noise pointer-events-none absolute inset-0 z-0 opacity-10" />

              {/* Content */}
              <div className="relative z-10 flex w-full max-w-2xl flex-col px-4">
                {/* Back link */}
                <m.div
                  initial={{ opacity: 0, x: -12 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
                  className="mb-4"
                >
                  <Link
                    href="/"
                    className="inline-flex items-center gap-1.5 text-xs font-medium text-white/40 transition-colors hover:text-white/70"
                  >
                    <ArrowLeft className="h-3.5 w-3.5" />
                    Back to Dashboard
                  </Link>
                </m.div>

                {/* Panel */}
                <m.div
                  initial={{ opacity: 0, y: 24 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.08, ease: [0.16, 1, 0.3, 1] }}
                  className="w-full overflow-hidden rounded-2xl border border-white/[0.08] bg-[#111318]"
                >
                  {/* Hero area */}
                  <div className="px-8 pt-10 pb-8">
                    <div className="flex items-center gap-2.5 mb-5">
                      <Image src="/Snowflake.svg" alt="Snowflake" width={20} height={20} className="h-5 w-5" />
                      <span className="text-sm font-semibold text-[#29B5E8]">Snowflake Migration</span>
                    </div>
                    <h1 className="text-[2.5rem] font-extrabold leading-[1.1] tracking-tight text-white">
                      Start a new<br />migration session
                    </h1>
                    <p className="mt-4 max-w-md text-[15px] leading-relaxed text-white/40">
                      Convert legacy SQL into Snowflake-ready output with AI-powered
                      transformation and built-in validation.
                    </p>

                    {/* Feature chips */}
                    <div className="mt-6 flex flex-wrap items-center gap-2">
                      {features.map((feat, i) => (
                        <span key={feat.label} className="flex items-center">
                          <span className="flex items-center gap-1.5 text-xs font-medium text-white/35">
                            <feat.icon className="h-3 w-3" />
                            {feat.label}
                          </span>
                          {i < features.length - 1 && (
                            <span className="mx-2 text-white/10">·</span>
                          )}
                        </span>
                      ))}
                    </div>
                  </div>

                  {/* Steps strip */}
                  <div className="border-t border-white/[0.06] bg-white/[0.02] px-8 py-5">
                    <div className="grid grid-cols-4 gap-3">
                      {steps.map((step, i) => (
                        <div key={step} className="flex flex-col items-center text-center gap-2">
                          <span className="flex h-7 w-7 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.04] text-xs font-bold text-white/40">
                            {i + 1}
                          </span>
                          <span className="text-[11px] leading-tight text-white/30">{step}</span>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* CTA strip */}
                  <div className="border-t border-white/[0.06] px-8 py-6">
                    <Link
                      href="/sessions"
                      className="group flex w-full items-center justify-center gap-2 rounded-xl bg-[#29B5E8] py-3.5 text-sm font-bold text-[#0a1628] transition-colors duration-200 hover:bg-[#24a3d4]"
                    >
                      Start Migration Session
                      <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-0.5" />
                    </Link>
                    <div className="mt-3 flex items-center justify-center gap-4 text-[11px] text-white/20">
                      <span className="flex items-center gap-1">
                        <Check className="h-3 w-3 text-emerald-400/40" />
                        15+ source databases
                      </span>
                      <span className="flex items-center gap-1">
                        <Check className="h-3 w-3 text-emerald-400/40" />
                        Enterprise-grade security
                      </span>
                    </div>
                  </div>
                </m.div>
              </div>
            </div>
          </SidebarInset>
        </div>
      </SidebarProvider>
    </div>
  );
}
