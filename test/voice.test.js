const request = require("supertest");
const app = require("../server");

describe("Voice Route", () => {
it("should return TwiML", async () => {
const res = await request(app)
.post("/voice")
.send({ From: "+61400000000" });

expect(res.statusCode).toBe(200);
expect(res.text).toContain("<Response>");
});
});
