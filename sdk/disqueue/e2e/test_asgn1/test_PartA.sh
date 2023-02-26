HOST=127.0.0.1
PORT=$1

curl -XGET "http://${HOST}:${PORT}/topics" 
curl -XPOST "http://${HOST}:${PORT}/topics?name=Kagenou"
curl -XPOST "http://${HOST}:${PORT}/topics?name=Kagenou"
curl -XPOST "http://${HOST}:${PORT}/topics?name=Minoru"
curl -XGET "http://${HOST}:${PORT}/topics" 
curl -XPOST "http://${HOST}:${PORT}/producer/register?topic=Minoru"
curl -XPOST "http://${HOST}:${PORT}/consumer/register?topic=Minoru"
