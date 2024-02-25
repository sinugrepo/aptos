spec aptos_framework::randomness {
    spec fetch_and_increment_txn_counter(): vector<u8> {
        pragma opaque;
    }

    spec is_unbiasable(): bool {
        pragma opaque;
    }
}
