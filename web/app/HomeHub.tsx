"use client";

import { useRouter } from "next/navigation";

const sports = [
  {
    key: "nba",
    label: "NBA",
    description: "Player props, grades, lineups",
    href: "/nba",
    available: true,
    color: "#C9A84C",
  },
  {
    key: "mlb",
    label: "MLB",
    description: "Box scores, game strip",
    href: "/mlb",
    available: true,
    color: "#4C8FC9",
  },
  {
    key: "nfl",
    label: "NFL",
    description: "Coming soon",
    href: null,
    available: false,
    color: "#6B6B6B",
  },
];

export default function HomeHub() {
  const router = useRouter();

  return (
    <main
      style={{
        minHeight: "100dvh",
        backgroundColor: "#0a0a0a",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        padding: "24px 16px",
        fontFamily: "'Georgia', serif",
        position: "relative",
      }}
    >
      <a
        href="/admin"
        style={{
          position: "absolute",
          top: "calc(env(safe-area-inset-top, 0px) + 16px)",
          right: 16,
          color: "#666",
          fontSize: 11,
          letterSpacing: "0.15em",
          textTransform: "uppercase",
          textDecoration: "none",
          padding: "6px 10px",
          border: "1px solid #222",
          borderRadius: 6,
        }}
      >
        admin
      </a>

      <p
        style={{
          color: "#444",
          fontSize: "11px",
          letterSpacing: "0.2em",
          textTransform: "uppercase",
          marginBottom: "48px",
          marginTop: 0,
        }}
      >
        schnapp
      </p>

      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "12px",
          width: "100%",
          maxWidth: "360px",
        }}
      >
        {sports.map((sport) => (
          <button
            key={sport.key}
            onClick={() => sport.href && router.push(sport.href)}
            disabled={!sport.available}
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              backgroundColor: "#111",
              border: `1px solid ${sport.available ? "#222" : "#1a1a1a"}`,
              borderRadius: "8px",
              padding: "20px 24px",
              cursor: sport.available ? "pointer" : "default",
              opacity: sport.available ? 1 : 0.4,
              transition: "background-color 0.15s, border-color 0.15s",
              width: "100%",
              textAlign: "left",
            }}
            onMouseEnter={(e) => {
              if (sport.available) {
                (e.currentTarget as HTMLButtonElement).style.backgroundColor = "#161616";
                (e.currentTarget as HTMLButtonElement).style.borderColor = "#333";
              }
            }}
            onMouseLeave={(e) => {
              if (sport.available) {
                (e.currentTarget as HTMLButtonElement).style.backgroundColor = "#111";
                (e.currentTarget as HTMLButtonElement).style.borderColor = "#222";
              }
            }}
          >
            <div>
              <div
                style={{
                  fontSize: "22px",
                  fontWeight: "600",
                  color: sport.available ? sport.color : "#555",
                  letterSpacing: "0.05em",
                  marginBottom: "4px",
                }}
              >
                {sport.label}
              </div>
              <div
                style={{
                  fontSize: "12px",
                  color: "#555",
                  letterSpacing: "0.02em",
                }}
              >
                {sport.description}
              </div>
            </div>
            {sport.available && (
              <svg
                width="16"
                height="16"
                viewBox="0 0 16 16"
                fill="none"
                style={{ color: "#444", flexShrink: 0 }}
              >
                <path
                  d="M6 3l5 5-5 5"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            )}
          </button>
        ))}
      </div>
    </main>
  );
}
