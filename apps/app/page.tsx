export default function Home() {
  return (
    <main style={{ padding: 40 }}>
      <h1>Auto Semantic Search MVP</h1>
      <input
        placeholder="BMW до 50 000 км, без окрасов"
        style={{ width: "100%", padding: 12, marginTop: 20 }}
      />
      <button style={{ marginTop: 12 }}>Найти</button>
    </main>
  );
}
