const express = require("express");
const sqlite3 = require("sqlite3");
const { open } = require("sqlite");

const app = express();
const port = 3000;

app.use(express.json());

let db;

const initializeDatabase = async () => {
  db = await open({
    filename: "./database.sqlite",
    driver: sqlite3.Database,
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS contacts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      phoneNumber TEXT,
      email TEXT,
      linkedId INTEGER,
      linkPrecedence TEXT,
      createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      deletedAt TIMESTAMP
    )
  `);
};

initializeDatabase();

const findContact = async (phoneNumber, email) => {
  const query = "SELECT * FROM contacts WHERE phoneNumber = ? OR email = ?";
  return await db.get(query, [phoneNumber, email]);
};

const createContact = async (phoneNumber, email, linkedId, linkPrecedence) => {
  const query = `
    INSERT INTO contacts (phoneNumber, email, linkedId, linkPrecedence)
    VALUES (?, ?, ?, ?)
  `;
  const result = await db.run(query, [
    phoneNumber,
    email,
    linkedId,
    linkPrecedence,
  ]);
  return result.lastID;
};

const updateContactLinkPrecedence = async (id, linkPrecedence) => {
  const query = "UPDATE contacts SET linkPrecedence = ? WHERE id = ?";
  await db.run(query, [linkPrecedence, id]);
};
const updateContactLinkedId = async (id, linkedId) => {
  const query = "UPDATE contacts SET linkedId = ? WHERE id = ?";
  await db.run(query, [linkedId, id]);
};

app.get("/", async (req, res) => {
  return res.send("Welcome to Bitespeed Assignment");
});

app.post("/identify", async (req, res) => {
  const { phoneNumber, email } = req.body;

  try {
    let existingContact = await findContact(phoneNumber, email);
    if (existingContact && existingContact.linkedId) {
      console.log(existingContact.linkedId);
      existingContact = await db.get("SELECT * FROM contacts WHERE id = ?", [
        existingContact.linkedId,
      ]);
      console.log(existingContact);
    }

    if (!existingContact) {
      // If no existing contact, create a new one as primary
      const id = await createContact(phoneNumber, email, null, "primary");
      res.json({
        contact: {
          primaryContactId: id,
          emails: [email],
          phoneNumbers: [phoneNumber],
          secondaryContactIds: [],
        },
      });
    } else {
      // If contact exists, update linkPrecedence and create a new contact as secondary
      await updateContactLinkPrecedence(existingContact.id, "primary");
      const linkedId = existingContact.id;
      //Check primary to secondary change required or not
      const multiplePrimaryContacts = await db.all(
        "SELECT * FROM contacts WHERE linkPrecedence = ? AND (phoneNumber = ? OR email = ?) ORDER BY createdAt",
        ["primary", phoneNumber, email]
      );
      if (multiplePrimaryContacts.length > 1) {
        const oldestContactId = multiplePrimaryContacts[0].id;
        const latestContactId = multiplePrimaryContacts[1].id;
        await updateContactLinkPrecedence(latestContactId, "secondary");
        await updateContactLinkedId(latestContactId, oldestContactId);
      } else {
        const newId = await createContact(
          phoneNumber,
          email,
          linkedId,
          "secondary"
        );
      }

      // Fetch all secondary contacts linked to the primary contact
      const secondaryContacts = await db.all(
        "SELECT * FROM contacts WHERE linkedId = ? AND linkPrecedence = 'secondary'",
        [linkedId]
      );
      //   console.log(secondaryContacts);

      // Extract emails and phone numbers
      const emails = [existingContact.email];
      const phoneNumbers = [existingContact.phoneNumber];
      const secondaryContactIds = [];
      for (let i = 0; i < secondaryContacts.length; i++) {
        let contact = secondaryContacts[i];
        if (contact.phoneNumber && !phoneNumbers.includes(contact.phoneNumber))
          phoneNumbers.push(contact.phoneNumber);
        if (contact.email && !emails.includes(contact.email))
          emails.push(contact.email);
        secondaryContactIds.push(contact.id);
      }
      //   secondaryContacts.map(
      //     (contact) => contact.id
      //   );

      res.json({
        contact: {
          primaryContactId: existingContact.id,
          emails,
          phoneNumbers,
          secondaryContactIds,
        },
      });
    }
  } catch (error) {
    console.error(error);
    res.status(500).send("Internal Server Error");
  }
});

app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});
