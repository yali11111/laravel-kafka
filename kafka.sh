#!/bin/bash

set -e

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT="ecommerce-kafka"

KAFKA_BROKER="localhost:9092"

echo "===================================================="
echo "     Laravel + Apache Kafka - Installation"
echo "===================================================="

# Vérification des dépendances
command -v php >/dev/null 2>&1 || {
    echo "Erreur : PHP n'est pas installé."
    exit 1
}

command -v composer >/dev/null 2>&1 || {
    echo "Erreur : Composer n'est pas installé."
    exit 1
}

command -v docker >/dev/null 2>&1 || {
    echo "Erreur : Docker n'est pas installé."
    exit 1
}

# ============================================================
# 1. CREATION DU PROJET LARAVEL
# ============================================================

echo ""
echo "[1/8] Création du projet Laravel..."

if [ -d "$PROJECT" ]; then
    echo "Le dossier $PROJECT existe déjà."
else
    mkdir -p "$PROJECT"

    cd "$PROJECT"

    composer create-project laravel/laravel .

fi

cd "$PROJECT"

echo "Projet Laravel créé."

# ============================================================
# 2. INSTALLATION LARAVEL KAFKA
# ============================================================

echo ""
echo "[2/8] Installation de Laravel Kafka..."

composer require mateusjunges/laravel-kafka

echo "Laravel Kafka installé."

# ============================================================
# 3. PUBLICATION DE LA CONFIGURATION
# ============================================================

echo ""
echo "[3/8] Publication de la configuration Kafka..."

php artisan vendor:publish \
    --provider="Junges\Kafka\KafkaServiceProvider" \
    --force || true

# ============================================================
# 4. CONFIGURATION ENV
# ============================================================

echo ""
echo "[4/8] Configuration du fichier .env..."

cat >> .env <<EOF

# ============================================================
# KAFKA
# ============================================================

KAFKA_BROKERS=${KAFKA_BROKER}
KAFKA_CONSUMER_GROUP_ID=ecommerce-group
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISMS=
KAFKA_AUTO_OFFSET_RESET=earliest

EOF

echo "Configuration Kafka ajoutée."

# ============================================================
# 5. CREATION DES PRODUCTEURS
# ============================================================

echo ""
echo "[5/8] Création du Producer..."

mkdir -p app/Kafka/Producers
mkdir -p app/Kafka/Consumers

cat > app/Kafka/Producers/OrderProducer.php <<'PHP'
<?php

namespace App\Kafka\Producers;

use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Message\Message;

class OrderProducer
{
    public static function publish(array $order): void
    {
        $message = new Message(
            body: $order,
            headers: [
                'event' => 'OrderCreated',
            ]
        );

        Kafka::publishOn('orders')
            ->withMessage($message)
            ->send();
    }
}
PHP

echo "OrderProducer créé."

# ============================================================
# 6. CREATION DU CONSUMER PAYMENT
# ============================================================

echo ""
echo "[6/8] Création du Consumer Payment..."

cat > app/Kafka/Consumers/PaymentConsumer.php <<'PHP'
<?php

namespace App\Kafka\Consumers;

use Junges\Kafka\Facades\Kafka;
use App\Models\Payment;

class PaymentConsumer
{
    public static function run(): void
    {
        $consumer = Kafka::consumer(
            ['orders'],
            env('KAFKA_CONSUMER_GROUP_ID', 'payment-service')
        )
        ->withHandler(function ($message) {

            $data = $message->getBody();

            echo "Order reçue : ";
            echo $data['order_id'] . PHP_EOL;

            Payment::create([
                'order_id' => $data['order_id'],
                'user_id' => $data['user_id'],
                'amount' => $data['amount'],
                'currency' => 'EUR',
                'status' => 'paid',
                'transaction_id' =>
                    'TXN-' . strtoupper(
                        bin2hex(random_bytes(6))
                    ),
            ]);

            echo "Payment créé." . PHP_EOL;
        })
        ->build();

        $consumer->consume();
    }
}
PHP

# ============================================================
# 7. CREATION DU CONSUMER NOTIFICATION
# ============================================================

echo ""
echo "[7/8] Création du Consumer Notification..."

cat > app/Kafka/Consumers/NotificationConsumer.php <<'PHP'
<?php

namespace App\Kafka\Consumers;

use Junges\Kafka\Facades\Kafka;
use App\Models\Notification;

class NotificationConsumer
{
    public static function run(): void
    {
        $consumer = Kafka::consumer(
            ['orders'],
            'notification-service'
        )
        ->withHandler(function ($message) {

            $data = $message->getBody();

            echo "Order reçue pour notification : ";
            echo $data['order_id'] . PHP_EOL;

            Notification::create([
                'user_id' => $data['user_id'],
                'type' => 'order_created',
                'recipient' => $data['email'],
                'subject' => 'Commande créée',
                'message' =>
                    'Votre commande #' .
                    $data['order_id'] .
                    ' a été créée.',
                'status' => 'sent',
            ]);

            echo "Notification créée." . PHP_EOL;
        })
        ->build();

        $consumer->consume();
    }
}
PHP

# ============================================================
# 8. DOCKER COMPOSE KAFKA
# ============================================================

echo ""
echo "[8/8] Création de Docker Compose..."

cat > docker-compose.yml <<'YAML'
services:

  kafka:
    image: apache/kafka:latest

    container_name: ecommerce-kafka

    ports:
      - "9092:9092"

    environment:

      KAFKA_NODE_ID: 1

      KAFKA_PROCESS_ROLES: broker,controller

      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      KAFKA_LISTENERS:
        PLAINTEXT://:9092,CONTROLLER://:9093

      KAFKA_ADVERTISED_LISTENERS:
        PLAINTEXT://localhost:9092

      KAFKA_CONTROLLER_LISTENER_NAMES:
        CONTROLLER

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

      KAFKA_NUM_PARTITIONS: 3
YAML

echo "Docker Compose créé."

# ============================================================
# CREATION DES COMMANDES ARTISAN
# ============================================================

echo ""
echo "Création des commandes Artisan..."

mkdir -p app/Console/Commands

cat > app/Console/Commands/ConsumePayments.php <<'PHP'
<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Kafka\Consumers\PaymentConsumer;

class ConsumePayments extends Command
{
    protected $signature = 'kafka:payments';

    protected $description = 'Consume order events for payments';

    public function handle()
    {
        PaymentConsumer::run();

        return Command::SUCCESS;
    }
}
PHP


cat > app/Console/Commands/ConsumeNotifications.php <<'PHP'
<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Kafka\Consumers\NotificationConsumer;

class ConsumeNotifications extends Command
{
    protected $signature = 'kafka:notifications';

    protected $description =
        'Consume order events for notifications';

    public function handle()
    {
        NotificationConsumer::run();

        return Command::SUCCESS;
    }
}
PHP

# ============================================================
# FIN
# ============================================================

echo ""
echo "===================================================="
echo "        INSTALLATION TERMINEE"
echo "===================================================="

echo ""
echo "Projet : $PROJECT"

echo ""
echo "Structure Kafka :"

echo "app/"
echo " └── Kafka/"
echo "     ├── Producers/"
echo "     │   └── OrderProducer.php"
echo "     │"
echo "     └── Consumers/"
echo "         ├── PaymentConsumer.php"
echo "         └── NotificationConsumer.php"

echo ""
echo "Démarrage de Kafka :"

echo "docker compose up -d"

echo ""
echo "Vérifier Kafka :"

echo "docker ps"

echo ""
echo "Lancer le consumer Payment :"

echo "php artisan kafka:payments"

echo ""
echo "Lancer le consumer Notification :"

echo "php artisan kafka:notifications"

echo ""
echo "===================================================="
echo "Kafka Broker : localhost:9092"
echo "Topic       : orders"
echo "===================================================="
