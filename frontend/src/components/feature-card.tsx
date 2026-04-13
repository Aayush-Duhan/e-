"use client";

import type { ComponentType, ReactNode } from "react";
import Link from "next/link";
import { m } from "framer-motion";
import { ArrowRight, Clock } from "lucide-react";
import { cn } from "@/lib/utils";

interface FeatureCardProps {
  title: string;
  description: string;
  icon: ComponentType<{ className?: string }>;
  href?: string;
  comingSoon?: boolean;
  className?: string;
  children?: ReactNode;
  delay?: number;
  accentColor?: string;
}

export function FeatureCard({
  title,
  description,
  icon: Icon,
  href,
  comingSoon = false,
  className,
  children,
  delay = 0,
  accentColor = "from-blue-500/20 to-cyan-500/20",
}: FeatureCardProps) {
  const inner = (
    <m.div
      initial={{ opacity: 0, y: 30, scale: 0.97 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.6, delay, ease: [0.16, 1, 0.3, 1] }}
      whileHover={
        comingSoon
          ? undefined
          : {
              scale: 1.02,
              transition: { duration: 0.3, ease: [0.16, 1, 0.3, 1] },
            }
      }
      className={cn(
        "group/card relative flex h-full flex-col overflow-hidden rounded-2xl border transition-all duration-500",
        comingSoon
          ? "cursor-not-allowed border-white/[0.06] bg-[#0d0f14]"
          : "cursor-pointer border-white/[0.08] bg-[#0f1117] hover:border-white/[0.15] hover:shadow-[0_0_60px_-12px_rgba(20,136,252,0.15)]",
        className,
      )}
    >
      {!comingSoon && (
        <div className="pointer-events-none absolute inset-0 rounded-2xl bg-gradient-to-br from-[#1488fc]/[0.03] via-transparent to-transparent opacity-0 transition-opacity duration-500 group-hover/card:opacity-100" />
      )}

      <div className="relative flex-1 overflow-hidden">
        {children}
        {comingSoon && (
          <div className="absolute inset-0 z-10 flex items-center justify-center">
            <div className="absolute inset-0 bg-[#0d0f14]/70 backdrop-blur-[3px]" />
            <m.div
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: delay + 0.3, duration: 0.4 }}
              className="relative z-10 flex items-center gap-2.5 rounded-full border border-white/10 bg-white/[0.04] px-5 py-2.5 backdrop-blur-md"
            >
              <Clock className="h-3.5 w-3.5 text-white/40" />
              <span className="text-xs font-medium tracking-wide text-white/50">
                Coming Soon
              </span>
            </m.div>
          </div>
        )}
      </div>

      <div
        className={cn(
          "relative z-10 space-y-3 p-5 pt-4",
          comingSoon && "opacity-50",
        )}
      >
        <div className="flex items-center gap-3">
          <div
            className={cn(
              "flex h-9 w-9 shrink-0 items-center justify-center rounded-xl",
              comingSoon
                ? "bg-white/[0.04] ring-1 ring-white/[0.06]"
                : "bg-gradient-to-br ring-1 ring-white/[0.08] " + accentColor,
            )}
          >
            <Icon
              className={cn(
                "h-[18px] w-[18px]",
                comingSoon ? "text-white/30" : "text-white/80",
              )}
            />
          </div>
          <h3
            className={cn(
              "text-[15px] font-semibold tracking-tight",
              comingSoon ? "text-white/40" : "text-white/90",
            )}
          >
            {title}
          </h3>
        </div>
        <p
          className={cn(
            "text-[13px] leading-relaxed",
            comingSoon ? "text-white/25" : "text-white/45",
          )}
        >
          {description}
        </p>
        {!comingSoon && href && (
          <div className="flex items-center gap-1.5 pt-0.5 text-[13px] font-medium text-[#1488fc]/80 transition-all group-hover/card:text-[#1488fc] group-hover/card:gap-2.5">
            Get Started
            <ArrowRight className="h-3.5 w-3.5 transition-transform group-hover/card:translate-x-0.5" />
          </div>
        )}
      </div>
    </m.div>
  );

  if (!comingSoon && href) {
    return (
      <Link href={href} className="block h-full">
        {inner}
      </Link>
    );
  }

  return inner;
}
