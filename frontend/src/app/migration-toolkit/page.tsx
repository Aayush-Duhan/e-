"use client";

import Link from "next/link";
import { motion } from "framer-motion";
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
              <div className="relative z-10 flex w-full max-w-[900px] flex-col items-center text-center">
                {/* Back to dashboard */}
                <motion.div
                  initial={{ opacity: 0, x: -12 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ duration: 0.4, ease: [0.16, 1, 0.3, 1] }}
                  className="mb-4 self-start"
                >
                  <Link
                    href="/"
                    className="inline-flex items-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.04] px-3 py-1.5 text-xs font-medium text-white/50 transition-all hover:border-white/20 hover:bg-white/[0.08] hover:text-white/80"
                  >
                    <ArrowLeft className="h-3.5 w-3.5" />
                    Dashboard
                  </Link>
                </motion.div>

                {/* Title */}
                <motion.h1
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.08, ease: [0.16, 1, 0.3, 1] }}
                  className="mt-5 text-4xl font-bold leading-[1.05] tracking-tight text-white sm:text-5xl"
                >
                  <span className="shimmer-text">Snowflake</span> Migration Hub
                </motion.h1>

                {/* Description */}
                <motion.p
                  initial={{ opacity: 0, y: 16 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.16, ease: [0.16, 1, 0.3, 1] }}
                  className="mt-3 max-w-[680px] text-base leading-relaxed text-gray-300 sm:text-lg"
                >
                  Set up a guided migration session to convert source SQL into
                  Snowflake-ready output using the built-in AI-powered toolkit.
                </motion.p>

                {/* Feature cards */}
                <motion.div
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.6, delay: 0.24, ease: [0.16, 1, 0.3, 1] }}
                  className="mt-8 grid w-full grid-cols-1 gap-3 sm:grid-cols-3"
                >
                  {features.map((feat, i) => (
                    <motion.div
                      key={feat.label}
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{
                        type: "spring",
                        stiffness: 300,
                        damping: 22,
                        delay: 0.3 + i * 0.08,
                      }}
                      className="group relative rounded-xl border border-white/[0.1] bg-white/[0.04] p-4 backdrop-blur-sm transition-all duration-300 hover:border-[#29B5E8]/40 hover:bg-white/[0.07]"
                    >
                      {/* Hover glow */}
                      <div className="pointer-events-none absolute -inset-[1px] rounded-xl opacity-0 transition-opacity duration-300 group-hover:opacity-100" style={{ boxShadow: "0 0 30px rgba(41,181,232,0.12), inset 0 0 20px rgba(41,181,232,0.05)" }} />
                      <div className="relative z-10">
                        <div className="mb-2.5 inline-flex h-9 w-9 items-center justify-center rounded-lg bg-[#29B5E8]/15 text-[#29B5E8] transition-transform duration-200 group-hover:scale-110">
                          <feat.icon className="h-4.5 w-4.5" />
                        </div>
                        <p className="text-sm font-semibold text-white">{feat.label}</p>
                        <p className="mt-1 text-xs leading-relaxed text-white/40">{feat.desc}</p>
                      </div>
                    </motion.div>
                  ))}
                </motion.div>

                {/* Steps pills */}
                <motion.div
                  initial={{ opacity: 0, y: 16 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.5, delay: 0.45, ease: [0.16, 1, 0.3, 1] }}
                  className="mt-7 flex flex-wrap items-center justify-center gap-2"
                >
                  {steps.map((step, i) => (
                    <span
                      key={step}
                      className="flex items-center gap-1.5 rounded-full border border-[#29B5E8]/20 bg-[#29B5E8]/[0.06] px-3 py-1.5 text-xs font-medium text-[#29B5E8]/80 sm:text-sm"
                    >
                      <span className="flex h-4.5 w-4.5 items-center justify-center rounded-full bg-[#29B5E8]/20 text-[10px] font-bold text-[#29B5E8]">
                        {i + 1}
                      </span>
                      {step}
                    </span>
                  ))}
                </motion.div>

                {/* CTA */}
                <motion.div
                  initial={{ opacity: 0, scale: 0.95 }}
                  animate={{ opacity: 1, scale: 1 }}
                  transition={{ duration: 0.5, delay: 0.55, ease: [0.16, 1, 0.3, 1] }}
                  className="mt-8"
                >
                  <Link
                    href="/sessions"
                    className="cta-pulse group inline-flex items-center gap-2.5 rounded-full bg-[#29B5E8] px-7 py-3 text-sm font-bold text-white shadow-[0_0_24px_rgba(41,181,232,0.35)] transition-all duration-300 hover:gap-3 hover:bg-[#24a3d4] hover:shadow-[0_0_32px_rgba(41,181,232,0.5)]"
                  >
                    <Snowflake className="h-4.5 w-4.5" />
                    Start Migration Session
                    <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-0.5" />
                  </Link>
                </motion.div>

                {/* Supported badge */}
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  transition={{ duration: 0.6, delay: 0.7 }}
                  className="mt-6 flex items-center gap-2 text-xs text-white/30"
                >
                  <Check className="h-3.5 w-3.5 text-emerald-400/60" />
                  15+ source databases supported
                  <span className="mx-1 text-white/15">|</span>
                  <Check className="h-3.5 w-3.5 text-emerald-400/60" />
                  Enterprise-grade security
                </motion.div>
              </div>
            </div>
          </SidebarInset>
        </div>
      </SidebarProvider>
    </div>
  );
}
