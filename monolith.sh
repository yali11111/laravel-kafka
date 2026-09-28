#!/bin/bash

set -e

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT="ecommerce-monolith-kafka"
KAFKA_BROKER="localhost:9092"

echo "======================================================"
echo " Laravel Monolithe + Apache Kafka"
echo "======================================================"

# ============================================================
# 1. VERIFICATION DES DEPENDANCES
# ============================================================

echo ""
echo "[1/10] Vérification des dépendances..."

command -v php >/dev/null 2>&1 || {
    echo "ERREUR : PHP n'est pas installé."
    exit 1
}

command -v composer >/dev/null 2>&1 || {
    echo "ERREUR : Composer n'est pas installé."
    exit 1
}

command -v docker >/dev/null 2>&1 || {
    echo "ERREUR : Docker n'est pas installé."
    exit 1
}

echo "PHP       : OK"
echo "Composer  : OK"
echo "Docker    : OK"

# ============================================================
# 2. CREATION DU PROJET LARAVEL
# ============================================================

echo ""
echo "[2/10] Création du projet Laravel..."

if [ -d "$PROJECT" ]; then
    echo "Le projet existe déjà."
else
    mkdir -p "$PROJECT"

    cd "$PROJECT"

    composer create-project laravel/laravel .

fi

cd "$PROJECT"

# ============================================================
# 3. INSTALLATION DE KAFKA
# ============================================================

echo ""
echo "[3/10] Installation de Laravel Kafka..."

composer require mateusjunges/laravel-kafka

php artisan vendor:publish \
    --provider="Junges\Kafka\KafkaServiceProvider" \
    --force || true

# ============================================================
# 4. CONFIGURATION ENV
# ============================================================

echo ""
echo "[4/10] Configuration Kafka..."

cat >> .env <<EOF

# ============================================================
# KAFKA
# ============================================================

KAFKA_BROKERS=${KAFKA_BROKER}
KAFKA_CONSUMER_GROUP_ID=monolith-group

EOF

# ============================================================
# 5. CREATION DES MODELES
# ============================================================

echo ""
echo "[5/10] Création des modèles..."

mkdir -p app/Models
mkdir -p app/Kafka/Producers
mkdir -p app/Kafka/Consumers
mkdir -p app/Console/Commands

# ------------------------------------------------------------
# Payment Model
# ------------------------------------------------------------

cat > app/Models/Payment.php <<'PHP'
<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Payment extends Model
{
    protected $fillable = [
        'order_id',
        'user_id',
        'amount',
        'currency',
        'status',
        'transaction_id',
    ];

    protected $casts = [
        'amount' => 'decimal:2',
    ];
}
PHP

# ------------------------------------------------------------
# Notification Model
# ------------------------------------------------------------

cat > app/Models/Notification.php <<'PHP'
<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Notification extends Model
{
    protected $fillable = [
        'user_id',
        'type',
        'recipient',
        'subject',
        'message',
        'status',
    ];
}
PHP

# ============================================================
# 6. MIGRATION PAYMENT
# ============================================================

echo ""
echo "[6/10] Création de la table payments..."

cat > database/migrations/2026_01_01_000001_create_payments_table.php <<'PHP'
<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('payments', function (Blueprint $table) {

            $table->id();

            $table->unsignedBigInteger('order_id');

            $table->unsignedBigInteger('user_id');

            $table->decimal('amount', 10, 2);

            $table->string('currency', 3)
                ->default('EUR');

            $table->string('status')
                ->default('pending');

            $table->string('transaction_id')
                ->nullable();

            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('payments');
    }
};
PHP

# ============================================================
# 7. MIGRATION NOTIFICATIONS
# ============================================================

echo ""
echo "[7/10] Création de la table notifications..."

cat > database/migrations/2026_01_01_000002_create_notifications_table.php <<'PHP'
<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('notifications', function (Blueprint $table) {

            $table->id();

            $table->unsignedBigInteger('user_id');

            $table->string('type');

            $table->string('recipient');

            $table->string('subject')
                ->nullable();

            $table->text('message');

            $table->string('status')
                ->default('pending');

            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('notifications');
    }
};
PHP

# ============================================================
# 8. PRODUCER ORDER
# ============================================================

echo ""
echo "[8/10] Création du Kafka Producer..."

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

# ============================================================
# 9. PAYMENT CONSUMER
# ============================================================

echo ""
echo "[9/10] Création du Payment Consumer..."

cat > app/Kafka/Consumers/PaymentConsumer.php <<'PHP'
<?php

namespace App\Kafka\Consumers;

use App\Models\Payment;
use Junges\Kafka\Facades\Kafka;

class PaymentConsumer
{
    public static function run(): void
    {
        $consumer = Kafka::consumer(
            ['orders'],
            'monolith-payment'
        )
        ->withHandler(function ($message) {

            $data = $message->getBody();

            echo PHP_EOL;
            echo "==================================" . PHP_EOL;
            echo "OrderCreated reçu" . PHP_EOL;
            echo "Order ID : "
                 . $data['order_id']
                 . PHP_EOL;
            echo "==================================" . PHP_EOL;

            Payment::create([
                'order_id' => $data['order_id'],
                'user_id' => $data['user_id'],
                'amount' => $data['amount'],
                'currency' => 'EUR',
                'status' => 'paid',
                'transaction_id' =>
                    'TXN-' .
                    strtoupper(
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
# NOTIFICATION CONSUMER
# ============================================================

cat > app/Kafka/Consumers/NotificationConsumer.php <<'PHP'
<?php

namespace App\Kafka\Consumers;

use App\Models\Notification;
use Junges\Kafka\Facades\Kafka;

class NotificationConsumer
{
    public static function run(): void
    {
        $consumer = Kafka::consumer(
            ['orders'],
            'monolith-notification'
        )
        ->withHandler(function ($message) {

            $data = $message->getBody();

            echo PHP_EOL;
            echo "==================================" . PHP_EOL;
            echo "OrderCreated reçu" . PHP_EOL;
            echo "Order ID : "
                 . $data['order_id']
                 . PHP_EOL;
            echo "==================================" . PHP_EOL;

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
# 10. COMMANDES ARTISAN
# ============================================================

echo ""
echo "[10/10] Création des commandes Kafka..."

cat > app/Console/Commands/KafkaPayment.php <<'PHP'
<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Kafka\Consumers\PaymentConsumer;

class KafkaPayment extends Command
{
    protected $signature = 'kafka:payment';

    protected $description =
        'Consume OrderCreated events for Payment';

    public function handle()
    {
        PaymentConsumer::run();

        return Command::SUCCESS;
    }
}
PHP


cat > app/Console/Commands/KafkaNotification.php <<'PHP'
<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Kafka\Consumers\NotificationConsumer;

class KafkaNotification extends Command
{
    protected $signature = 'kafka:notification';

    protected $description =
        'Consume OrderCreated events for Notification';

    public function handle()
    {
        NotificationConsumer::run();

        return Command::SUCCESS;
    }
}
PHP

# ============================================================
# DOCKER KAFKA
# ============================================================

echo ""
echo "Création de docker-compose.yml..."

cat > docker-compose.yml <<'YAML'
services:

  kafka:
    image: apache/kafka:latest

    container_name: monolith-kafka

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

# ============================================================
# FIN
# ============================================================

echo ""
echo "======================================================"
echo "       INSTALLATION TERMINEE"
echo "======================================================"

echo ""
echo "Projet : $PROJECT"

echo ""
echo "Structure :"

echo "app/"
echo "├── Kafka/"
echo "│   ├── Producers/"
echo "│   │   └── OrderProducer.php"
echo "│   └── Consumers/"
echo "│       ├── PaymentConsumer.php"
echo "│       └── NotificationConsumer.php"
echo "│"
echo "└── Models/"
echo "    ├── Payment.php"
echo "    └── Notification.php"

echo ""
echo "Démarrer Kafka :"
echo "docker compose up -d"

echo ""
echo "Migration :"
echo "php artisan migrate"

echo ""
echo "Payment Consumer :"
echo "php artisan kafka:payment"

echo ""
echo "Notification Consumer :"
echo "php artisan kafka:notification"

echo ""
echo "======================================================"
echo " Kafka : localhost:9092"
echo " Topic : orders"
echo "======================================================"
