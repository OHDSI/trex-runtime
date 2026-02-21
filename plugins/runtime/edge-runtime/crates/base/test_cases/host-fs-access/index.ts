export default {
  async fetch() {
    try {
      const content = await Deno.readTextFile("/etc/hostname");
      return Response.json({ ok: true, content: content.trim() });
    } catch (e) {
      return Response.json({ ok: false, error: e.toString() }, { status: 500 });
    }
  },
};
