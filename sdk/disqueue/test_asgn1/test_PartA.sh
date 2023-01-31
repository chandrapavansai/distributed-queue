HOST=127.0.0.1
PORT=$1

curl -XGET "http://${HOST}:${PORT}/topics" 
curl -XPOST "http://${HOST}:${PORT}/topics" -d '{"topic_name": "Kagenou"}' -H "Content-Type: application/json"
curl -XPOST "http://${HOST}:${PORT}/topics" -d '{"topic_name": "Kagenou"}' -H "Content-Type: application/json"
curl -XPOST "http://${HOST}:${PORT}/topics" -d '{"topic_name": "Minoru"}' -H "Content-Type: application/json"
curl -XGET "http://${HOST}:${PORT}/topics" 
curl -XPOST "http://${HOST}:${PORT}/producer/register" -d '{"topic": "Minoru"}' -H "Content-Type: application/json"
curl -XPOST "http://${HOST}:${PORT}/consumer/register" -d '{"topic": "Minoru"}' -H "Content-Type: application/json"
