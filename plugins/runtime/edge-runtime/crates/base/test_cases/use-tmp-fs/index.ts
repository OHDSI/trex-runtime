const meowFile = await Deno.create("/tmp/meow");
const meow = new TextEncoder().encode("meowmeow");

export default {
  async fetch() {
    const written = await meowFile.write(meow);
    const content = await Deno.readTextFile("/tmp/meow");
    const hadExisted = await exists("/tmp/meow");
    let deleted = false;

    try {
      await Deno.remove("/tmp/meow");
      deleted = true;
    } catch {
      // empty
    }

    return Response.json({
      written,
      content,
      deleted,
      steps: [
        hadExisted,
        await exists("/tmp/nonexistent"),
      ],
    });
  },
};

async function exists(path: string) {
  try {
    await Deno.stat(path);
    return true;
  } catch {
    return false;
  }
}
