import { Lyra } from "../src/lyra";

function getId({ id }: { id: string }) {
  return id;
}

describe("defaultLanguage", () => {
  it("should throw an error if the desired language is not supported", () => {
    try {
      new Lyra({
        schema: {},
        // @ts-expect-error latin is not supported
        defaultLanguage: "latin",
      });
    } catch (e) {
      expect(e).toMatchSnapshot();
    }
  });

  it("should throw an error if the desired language is not supported during insertion", async () => {
    try {
      const db = new Lyra({
        schema: { foo: "string" },
      });

      await db.insert(
        {
          foo: "bar",
        },
        // @ts-expect-error latin is not supported
        "latin"
      );
    } catch (e) {
      expect(e).toMatchSnapshot();
    }
  });

  it("should not throw if if the language is supported", () => {
    try {
      new Lyra({
        schema: {},
        defaultLanguage: "portugese",
      });
    } catch (e) {
      expect(e).toBeUndefined();
    }
  });
});

describe("checkInsertDocSchema", () => {
  it("should compare the inserted doc with the schema definition", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: "string",
      },
    });

    expect(
      await db.insert({ quote: "hello, world!", author: "me" }).then(getId)
    ).toBeDefined();

    try {
      // @ts-expect-error test error case
      await db.insert({ quote: "hello, world!", author: true });
    } catch (err) {
      expect(err).toMatchSnapshot();
    }

    try {
      await db.insert({
        quote: "hello, world!",
        // @ts-expect-error test error case
        authors: "author should be singular",
      });
    } catch (err) {
      expect(err).toMatchSnapshot();
    }

    try {
      // @ts-expect-error test error case
      await db.insert({ quote: "hello, world!", foo: { bar: 10 } });
    } catch (err) {
      expect(err).toMatchSnapshot();
    }
  });
});

describe("lyra", () => {
  it("should correctly insert and retrieve data", async () => {
    const db = new Lyra({
      schema: {
        example: "string",
      },
    });

    const ex1Insert = await db.insert({ example: "The quick, brown, fox" });
    const ex1Search = await db.search({
      term: "quick",
      properties: ["example"],
    });

    expect(ex1Insert.id).toBeDefined();
    expect(ex1Search.count).toBe(1);
    expect(ex1Search.elapsed).toBeDefined();
    expect(ex1Search.hits[0].example).toBe("The quick, brown, fox");
  });

  it("should correctly paginate results", async () => {
    const db = new Lyra({
      schema: {
        animal: "string",
      },
    });

    await db.insert({ animal: "Quick brown fox" });
    await db.insert({ animal: "Lazy dog" });
    await db.insert({ animal: "Jumping penguin" });
    await db.insert({ animal: "Fast chicken" });
    await db.insert({ animal: "Fabolous ducks" });
    await db.insert({ animal: "Fantastic horse" });

    const search1 = await db.search({ term: "f", limit: 1, offset: 0 });
    const search2 = await db.search({ term: "f", limit: 1, offset: 1 });
    const search3 = await db.search({ term: "f", limit: 1, offset: 2 });
    const search4 = await db.search({ term: "f", limit: 2, offset: 2 });

    expect(search1.count).toBe(4);
    expect(search1.hits[0].animal).toBe("Quick brown fox");

    expect(search2.count).toBe(4);
    expect(search2.hits[0].animal).toBe("Fast chicken");

    expect(search3.count).toBe(4);
    expect(search3.hits[0].animal).toBe("Fabolous ducks");

    expect(search4.count).toBe(4);
    expect(search4.hits[0].animal).toBe("Fabolous ducks");
    expect(search4.hits[1].animal).toBe("Fantastic horse");
  });

  it("Should throw an error when searching in non-existing indices", async () => {
    const db = new Lyra({ schema: { foo: "string", baz: "string" } });

    try {
      await db.search({
        term: "foo",
        //@ts-expect-error test error case
        properties: ["bar"],
      });
    } catch (err) {
      expect(err).toMatchInlineSnapshot(
        `"Invalid property name. Expected a wildcard string (\\"*\\") or array containing one of the following properties: foo, baz, but got: bar"`
      );
    }
  });

  it("Should correctly remove a document after its insertion", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: "string",
      },
    });

    const { id: id1 } = await db.insert({
      quote: "Be yourself; everyone else is already taken.",
      author: "Oscar Wilde",
    });

    const { id: id2 } = await db.insert({
      quote:
        "To live is the rarest thing in the world. Most people exist, that is all.",
      author: "Oscar Wilde",
    });

    await db.insert({
      quote: "So many books, so little time.",
      author: "Frank Zappa",
    });

    const res = await db.delete(id1);

    const searchResult = await db.search({
      term: "Oscar",
      properties: ["author"],
    });

    expect(res).toBeTruthy();
    expect(searchResult.count).toBe(1);
    expect(searchResult.hits[0].author).toBe("Oscar Wilde");
    expect(searchResult.hits[0].quote).toBe(
      "To live is the rarest thing in the world. Most people exist, that is all."
    );
    expect(searchResult.hits[0].id).toBe(id2);
  });

  it("Should preserve identical docs after deletion", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: "string",
      },
    });

    const { id: id1 } = await db.insert({
      quote: "Be yourself; everyone else is already taken.",
      author: "Oscar Wilde",
    });

    const { id: id2 } = await db.insert({
      quote: "Be yourself; everyone else is already taken.",
      author: "Oscar Wilde",
    });

    await db.insert({
      quote: "So many books, so little time.",
      author: "Frank Zappa",
    });

    const res = await db.delete(id1);

    const searchResult = await db.search({
      term: "Oscar",
      properties: ["author"],
    });

    const searchResult2 = await db.search({
      term: "already",
      properties: ["quote"],
    });

    expect(res).toBeTruthy();
    expect(searchResult.count).toBe(1);
    expect(searchResult.hits[0].author).toBe("Oscar Wilde");
    expect(searchResult.hits[0].quote).toBe(
      "Be yourself; everyone else is already taken."
    );
    expect(searchResult.hits[0].id).toBe(id2);

    expect(searchResult2.count).toBe(1);
    expect(searchResult2.hits[0].author).toBe("Oscar Wilde");
    expect(searchResult2.hits[0].quote).toBe(
      "Be yourself; everyone else is already taken."
    );
    expect(searchResult2.hits[0].id).toBe(id2);
  });

  it("Should be able to insert documens with non-searchable fields", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: "string",
        isFavorite: "boolean",
        rating: "number",
      },
    });

    await db.insert({
      quote: "Be yourself; everyone else is already taken.",
      author: "Oscar Wilde",
      isFavorite: false,
      rating: 4,
    });

    await db.insert({
      quote: "So many books, so little time.",
      author: "Frank Zappa",
      isFavorite: true,
      rating: 5,
    });

    const search = await db.search({
      term: "frank",
    });

    expect(search.count).toBe(1);
    expect(search.hits[0].author).toBe("Frank Zappa");
  });

  it("Should exact match", async () => {
    const db = new Lyra({
      schema: {
        author: "string",
        quote: "string",
      },
    });

    await db.insert({
      quote: "Be yourself; everyone else is already taken.",
      author: "Oscar Wilde",
    });

    const partialSearch = await db.search({
      term: "alr",
      exact: true,
    });

    expect(partialSearch.count).toBe(0);

    const exactSearch = await db.search({
      term: "already",
      exact: true,
    });

    expect(exactSearch.count).toBe(1);
    expect(exactSearch.hits[0].quote).toBe(
      "Be yourself; everyone else is already taken."
    );
    expect(exactSearch.hits[0].author).toBe("Oscar Wilde");
  });

  it("Shouldn't tolerate typos", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: "string",
      },
    });

    await db.insert({
      quote:
        "Absolutely captivating creatures, seahorses seem like a product of myth and imagination rather than of nature.",
      author: "Sara A. Lourie",
    });

    const search = await db.search({
      term: "seahrse",
      tolerance: 0,
    });

    expect(search.count).toBe(0);
  });

  it("Should tolerate typos", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: "string",
      },
    });

    await db.insert({
      quote:
        "Absolutely captivating creatures, seahorses seem like a product of myth and imagination rather than of nature.",
      author: "Sara A. Lourie",
    });

    await db.insert({
      quote:
        "Seahorses look mythical, like dragons, but these magnificent shy creatures are real.",
      author: "Jennifer Keats Curtis",
    });

    const tolerantSearch = await db.search({
      term: "seahrse",
      tolerance: 1,
    });

    expect(tolerantSearch.count).toBe(2);

    const moreTolerantSearch = await db.search({
      term: "sahrse",
      tolerance: 3,
    });

    expect(moreTolerantSearch.count).toBe(2);
  });

  it("Should support nested properties", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
        author: {
          name: "string",
          surname: "string",
        },
      },
    });

    await db.insert({
      quote: "Harry Potter, the boy who lived, come to die. Avada kedavra.",
      author: {
        name: "Tom",
        surname: "Riddle",
      },
    });

    await db.insert({
      quote: "I am Homer Simpson.",
      author: {
        name: "Homer",
        surname: "Simpson",
      },
    });

    const resultAuthorSurname = await db.search({
      term: "Riddle",
      properties: ["author.surname"],
    });

    const resultAuthorName = await db.search({
      term: "Riddle",
      properties: ["author.name"],
    });

    const resultSimpsonQuote = await db.search({
      term: "Homer",
      properties: ["quote"],
    });

    const resultSimpsonAuthorName = await db.search({
      term: "Homer",
      properties: ["author.name"],
    });

    expect(resultSimpsonAuthorName.count).toBe(1);
    expect(resultSimpsonQuote.count).toBe(1);
    expect(resultAuthorSurname.count).toBe(1);
    expect(resultAuthorName.count).toBe(0);
  });
});

describe("Disable stemming", () => {
  it("Should disable stemming globally", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
      },
      stemming: false,
    });

    await db.insert({
      quote: "I am baking some cakes",
    });

    const result1 = await db.search({
      term: "bake",
      exact: true,
    });

    const result2 = await db.search({
      term: "bak",
      tolerance: 10,
    });

    // Resoults should be empty as "baking" doesn't get reduced to "bake"
    expect(result1.count).toBe(0);
    expect(result2.count).toBe(1);
  });

  it("Should stem by default", async () => {
    const db = new Lyra({
      schema: {
        quote: "string",
      },
    });

    await db.insert({
      quote: "I am baking some cakes",
    });

    const result1 = await db.search({
      term: "bake",
      exact: true,
    });

    expect(result1.count).toBe(1);
  });
});
