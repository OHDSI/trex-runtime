import express from 'npm:express';

const app = express();

app.get('/express', (req, res) => {

	res.send('Welcome to the Dinosaur API!asd');
});

app.listen(8000);
