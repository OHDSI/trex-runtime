import express from 'npm:express';

const app = express();

app.get('/sql', (req, res) => {
	console.log("asdasdas");
    const conn = new Trex.TrexDB("test");
    let rows = conn.execute("select * from demo_cdm.person");
	console.log("asdasdas2");

	res.send("\n\n count: "+rows.length+ "\n\n"+JSON.stringify(rows, null , 2) );
});

app.listen(8000);
