"use client";

import { useRouter } from "next/navigation";
import Image from "next/image";
import { m } from "framer-motion";
import { Header } from "@/components/header";
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar";
import { SessionSidebar } from "@/components/session-sidebar";
import {
  ArrowRight,
  Check,
  Database,
  Layers,
} from "lucide-react";

/* ------------------------------------------------------------------ */
/*  Card data                                                          */
/* ------------------------------------------------------------------ */

function SnowflakeLogo({ className }: { className?: string; style?: React.CSSProperties }) {
  return <Image src="/Snowflake.svg" alt="Snowflake" width={24} height={24} className={className} />;
}

interface MigrationCard {
  title: string;
  icon: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
  color: string;
  description: string;
  supports: string[];
  inputs: string[];
  status: "active" | "coming_soon";
  href?: string;
  bgImage?: string;
}

const migrationCards: MigrationCard[] = [
  {
    title: "Snowflake Migration",
    icon: SnowflakeLogo,
    color: "#29B5E8",
    description:
      "Migrate and transform SQL scripts, stored procedures, and data pipelines into Snowflake-compatible syntax with automated validation.",
    supports: [
      "SQL Scripts",
      "Stored Procedures",
      "User-Defined Functions",
      "Views & Materialized Views",
      "Data Pipelines",
    ],
    inputs: ["Source SQL scripts", "Target Snowflake account"],
    status: "active",
    href: "/migration-toolkit",
    bgImage: "/bg-snowflake.png",
  },
  {
    title: "Databricks Migration",
    icon: Database,
    color: "#FF3621",
    description:
      "Convert and migrate PySpark notebooks, Delta Lake configurations, and Unity Catalog assets to your Databricks workspace.",
    supports: [
      "PySpark Notebooks",
      "Delta Tables",
      "Unity Catalog",
      "SQL Warehouses",
      "Workflow Jobs",
    ],
    inputs: ["Workspace URL", "Access Token", "Source scripts"],
    status: "coming_soon",
    bgImage: "/bg-databricks.png",
  },
  {
    title: "BigQuery Migration",
    icon: Layers,
    color: "#4285F4",
    description:
      "Transform standard SQL, scheduled queries, and materialized views for seamless deployment to Google BigQuery.",
    supports: [
      "Standard SQL",
      "Scheduled Queries",
      "Materialized Views",
      "User-Defined Functions",
      "Data Transfers",
    ],
    inputs: ["GCP Project ID", "Dataset ID", "Source scripts"],
    status: "coming_soon",
    bgImage: "/bg-bigquery.png",
  },
];

/* ------------------------------------------------------------------ */
/*  Dashboard page                                                     */
/* ------------------------------------------------------------------ */

export default function DashboardPage() {
  const router = useRouter();

  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-[#1a1a1a]"
      style={{ ["--header-h" as string]: "48px" }}
    >
      <Header />

      <SidebarProvider className="sidebar-offset min-h-0 flex-1">
        <div className="flex min-h-0 w-full flex-1">
          <SessionSidebar />

          <SidebarInset className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#0f1219]">
            <div className="relative flex min-h-0 flex-1 overflow-hidden">
              {/* Background image (subtle) */}
              <div
                className="pointer-events-none absolute inset-0 opacity-[0.07]"
                style={{
                  backgroundImage: "url('/bg.png')",
                  backgroundSize: "cover",
                  backgroundPosition: "center",
                }}
              />

              {/* Static gradient background */}
              <div
                className="pointer-events-none absolute inset-0"
                style={{
                  background: `
                    linear-gradient(135deg,
                      rgba(167,139,250,0.08) 0%,
                      rgba(15,18,25,0.85) 25%,
                      rgba(20,24,35,0.95) 50%,
                      rgba(15,18,25,0.85) 75%,
                      rgba(167,139,250,0.06) 100%
                    ),
                    #0f1219
                  `,
                }}
              />
              {/* Neutral radial gradient */}
              <div
                className="pointer-events-none absolute inset-0"
                style={{
                  background:
                    "radial-gradient(ellipse at 30% 20%, rgba(255,255,255,0.03) 0%, transparent 60%), radial-gradient(ellipse at 70% 80%, rgba(255,255,255,0.02) 0%, transparent 50%)",
                }}
              />
              {/* Noise texture */}
              <div className="bg-noise pointer-events-none absolute inset-0 opacity-10" />

              {/* ── Content ── */}
              <div className="relative z-[2] flex h-full w-full flex-col overflow-hidden px-4 py-4 sm:px-6 md:px-10">
                <div className="mx-auto flex w-full max-w-7xl flex-1 flex-col min-h-0">
                  {/* ── Section header ── */}
                  <m.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.6, ease: [0.16, 1, 0.3, 1] }}
                    className="mb-4 shrink-0"
                  >
                    <div className="flex flex-col gap-1">
                      <span className="text-md text-[#FFE600] font-semibold">
                        Powered by ETHAN
                      </span>
                      <h1 className="text-4xl font-bold text-white sm:text-3xl">
                        Agent Driven Legacy Migration Suite
                      </h1>
                    </div>
                    <p className="mt-2 max-w-2xl text-sm leading-relaxed text-gray-300">
                      Enterprise-grade migration toolkit for transforming and
                      migrating SQL scripts, stored procedures, and data
                      pipelines across cloud data platforms.
                    </p>
                  </m.div>

                  {/* ── 3-column card grid ── */}
                  <div className="grid min-h-0 flex-1 grid-cols-1 gap-4 pb-2 md:grid-cols-3">
                    {migrationCards.map((card, i) => (
                      <m.div
                        key={card.title}
                        initial={{ opacity: 0, y: 30 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{
                          type: "spring",
                          stiffness: 300,
                          damping: 22,
                          delay: 0.15 + i * 0.1,
                        }}
                        className="group relative flex cursor-pointer flex-col overflow-hidden rounded-2xl border border-white/[0.15] bg-white/[0.06] p-5 transition-[transform,background-color,border-color] duration-300 ease-out will-change-transform hover:scale-[1.02] hover:border-white/30 hover:bg-white/[0.10]"
                        style={{
                          ["--card-color" as string]: card.color,
                        }}
                        onClick={() => {
                          if (card.status === "active" && card.href) {
                            router.push(card.href);
                          }
                        }}
                        role={card.status === "active" ? "link" : undefined}
                      >
                        {/* Static backdrop blur layer (doesn't scale with card) */}
                        <div className="pointer-events-none absolute inset-0 rounded-2xl backdrop-blur-md" />
                        {/* Hover glow overlay */}
                        <div
                          className="pointer-events-none absolute -inset-[1px] rounded-2xl opacity-0 transition-opacity duration-300 group-hover:opacity-100"
                          style={{
                            boxShadow: `0 0 40px ${card.color}30, 0 0 80px ${card.color}15, inset 0 0 30px ${card.color}08`,
                          }}
                        />

                        {/* Hover border glow */}
                        <div
                          className="pointer-events-none absolute -inset-[1px] rounded-2xl border opacity-0 transition-opacity duration-300 group-hover:opacity-100"
                          style={{ borderColor: `${card.color}60` }}
                        />

                        {/* Decorative background image */}
                        {card.bgImage && (
                          <div
                            className="pointer-events-none absolute inset-0 rounded-2xl opacity-[0.3] transition-opacity duration-300 group-hover:opacity-[0.55]"
                            style={{
                              backgroundImage: `url('${card.bgImage}')`,
                              backgroundSize: "cover",
                              backgroundPosition: "center",
                            }}
                          />
                        )}

                        {/* Card content */}
                        <div className="relative z-10 flex flex-1 flex-col min-h-0">
                          {/* Icon */}
                          <div
                            className="mb-3 inline-flex h-10 w-10 items-center justify-center rounded-xl shrink-0 transition-transform duration-200 group-hover:rotate-[6deg] group-hover:scale-110"
                            style={{
                              backgroundColor: `${card.color}20`,
                              border: `1px solid ${card.color}40`,
                            }}
                          >
                            <card.icon
                              className="h-6 w-6"
                              style={{ color: card.color }}
                            />
                          </div>

                          {/* Title + status */}
                          <div className="mb-2 flex items-center justify-between">
                            <h2 className="text-xl font-bold text-white">
                              {card.title}
                            </h2>
                            {card.status === "active" ? (
                              <span className="inline-flex items-center rounded-full bg-emerald-500/25 px-3 py-1 text-xs font-bold uppercase tracking-wide text-emerald-300">
                                Active
                              </span>
                            ) : (
                              <span className="inline-flex items-center rounded-full bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-gray-400">
                                Coming Soon
                              </span>
                            )}
                          </div>

                          {/* Description */}
                          <p className="mb-3 text-sm leading-relaxed text-gray-300">
                            {card.description}
                          </p>

                          {/* Supports */}
                          <div className="mb-3">
                            <h3
                              className="mb-1.5 text-xs font-semibold uppercase tracking-wider"
                              style={{ color: card.color }}
                            >
                              Supports
                            </h3>
                            <ul className="space-y-1">
                              {card.supports.map((item) => (
                                <li
                                  key={item}
                                  className="flex items-center gap-2 text-xs text-gray-300"
                                >
                                  <Check
                                    className="h-3.5 w-3.5 shrink-0"
                                    style={{ color: card.color }}
                                  />
                                  {item}
                                </li>
                              ))}
                            </ul>
                          </div>

                          {/* Required Inputs */}
                          <div className="mb-3">
                            <h3 className="mb-1.5 text-xs font-semibold uppercase tracking-wider text-gray-400">
                              Required Inputs
                            </h3>
                            <ul className="space-y-1">
                              {card.inputs.map((input) => (
                                <li
                                  key={input}
                                  className="text-xs text-gray-400"
                                >
                                  <span className="mr-2 text-gray-500">
                                    &bull;
                                  </span>
                                  {input}
                                </li>
                              ))}
                            </ul>
                          </div>

                          {/* Spacer to push CTA to bottom */}
                          <div className="flex-1" />

                          {/* CTA / Status footer */}
                          {card.status === "active" ? (
                            <div
                              className="inline-flex items-center gap-2 self-start rounded-full px-4 py-2 text-sm font-bold text-white shadow-md transition-all duration-300 group-hover:gap-3"
                              style={{
                                backgroundColor: card.color,
                                boxShadow: `0 0 20px ${card.color}40`,
                              }}
                            >
                              Start Migration
                              <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-0.5" />
                            </div>
                          ) : (
                            <div className="inline-flex items-center gap-2 self-start rounded-full border border-white/15 bg-white/[0.06] px-4 py-2 text-sm font-medium text-gray-400">
                              Under Development
                            </div>
                          )}
                        </div>
                      </m.div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </SidebarInset>
        </div>
      </SidebarProvider>
    </div>
  );
}
