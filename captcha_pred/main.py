import os
import numpy as np
import datetime
import matplotlib.pyplot as plt

from pathlib import Path
from collections import Counter

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

from ctclayer import CTCLayer
from utils import get_lst_images, split_data

training = True

log_dir = r"E:\automating_reports_V2\automation\captcha_pred\logs\fit" + \
    datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

checkpoint_dir = r'E:\automating_reports_V2\automation\captcha_pred\checkpoints\best.ckpt'

model_save_dir = r'E:\automating_reports_V2\automation\captcha_pred\saved_models\captcha_model'


cp_callback = tf.keras.callbacks.ModelCheckpoint(filepath=checkpoint_dir,
                                                 save_weights_only=True,
                                                 verbose=1)

tensorboard_callback = tf.keras.callbacks.TensorBoard(
    log_dir=log_dir, histogram_freq=1)


# Path to the data directory
data_dir = Path(
    r"E:\automating_reports_V2\saved_dir\codeghtesadi\img_files\training")

data_dir_prod = Path(
    r"E:\automating_reports_V2\saved_dir\codeghtesadi\img_files\production")


images, labels, characters = get_lst_images(data_dir)

for item in labels:
    if len(item) != 4:
        print(item)

# Batch size for training and validation
batch_size = 16
# Desired image dimensions
img_width = 200
img_height = 50

# Factor by which the image is going to be downsampled
# by the convolutional blocks. We will be using two
# convolution blocks and each block will have
# a pooling layer which downsample the features by a factor of 2.
# Hence total downsampling factor would be 4.
downsample_factor = 4

# Maximum length of any captcha in the dataset
max_length = max([len(label) for label in labels])


# Mapping characters to integers
char_to_num = layers.StringLookup(
    vocabulary=list(characters), mask_token=None
)

# Mapping integers back to original characters
num_to_char = layers.StringLookup(
    vocabulary=char_to_num.get_vocabulary(), mask_token=None, invert=True
)


# Splitting data into training and validation sets
x_train, x_valid, y_train, y_valid = split_data(
    np.array(images), np.array(labels))


def encode_single_sample(img_path, label):
    # 1. Read image
    img = tf.io.read_file(img_path)
    # 2. Decode and convert to grayscale
    img = tf.io.decode_png(img, channels=1)
    # 3. Convert to float32 in [0, 1] range
    img = tf.image.convert_image_dtype(img, tf.float32)
    # 4. Resize to the desired size
    img = tf.image.resize(img, [img_height, img_width])
    # 5. Transpose the image because we want the time
    # dimension to correspond to the width of the image.
    img = tf.transpose(img, perm=[1, 0, 2])
    # 6. Map the characters in label to numbers
    label = char_to_num(tf.strings.unicode_split(
        label, input_encoding="UTF-8"))
    # 7. Return a dict as our model is expecting two inputs
    return {"image": img, "label": label}


data_augmentation = tf.keras.Sequential([
    layers.RandomFlip("horizontal_and_vertical"),
    layers.RandomRotation(0.2),
])


train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))


train_dataset = (
    train_dataset.map(
        encode_single_sample, num_parallel_calls=tf.data.AUTOTUNE
    )
    .batch(batch_size)
    .prefetch(buffer_size=tf.data.AUTOTUNE)
)


validation_dataset = tf.data.Dataset.from_tensor_slices((x_valid, y_valid))
validation_dataset = (
    validation_dataset.map(
        encode_single_sample, num_parallel_calls=tf.data.AUTOTUNE
    )
    .batch(batch_size)
    .prefetch(buffer_size=tf.data.AUTOTUNE)
)


def visulize_data():
    _, ax = plt.subplots(4, 4, figsize=(10, 5))
    for batch in train_dataset.take(1):
        images = batch["image"]
        labels = batch["label"]
        for i in range(16):
            img = (images[i] * 255).numpy().astype("uint8")
            label = tf.strings.reduce_join(
                num_to_char(labels[i])).numpy().decode("utf-8")
            ax[i // 4, i % 4].imshow(img[:, :, 0].T, cmap="gray")
            ax[i // 4, i % 4].set_title(label)
            ax[i // 4, i % 4].axis("off")
    plt.show()


def build_model():
    # Inputs to the model
    input_img = layers.Input(
        shape=(img_width, img_height, 1), name="image", dtype="float32"
    )
    labels = layers.Input(name="label", shape=(None,), dtype="float32")
    # input_img = data_augmentation(input_img)
    # First conv block
    x = layers.Conv2D(
        32,
        (3, 3),
        activation="relu",
        kernel_initializer="he_normal",
        padding="same",
        name="Conv1",
    )(input_img)
    x = layers.MaxPooling2D((2, 2), name="pool1")(x)

    # Second conv block
    x = layers.Conv2D(
        64,
        (3, 3),
        activation="relu",
        kernel_initializer="he_normal",
        padding="same",
        name="Conv2",
    )(x)
    x = layers.MaxPooling2D((2, 2), name="pool2")(x)

    # We have used two max pool with pool size and strides 2.
    # Hence, downsampled feature maps are 4x smaller. The number of
    # filters in the last layer is 64. Reshape accordingly before
    # passing the output to the RNN part of the model
    new_shape = ((img_width // 4), (img_height // 4) * 64)
    x = layers.Reshape(target_shape=new_shape, name="reshape")(x)
    x = layers.Dense(128, activation="relu", name="dense1")(x)
    x = layers.Dropout(0.4)(x)
    x = layers.Dense(128, activation="relu", name="dense1x")(x)
    x = layers.Dropout(0.4)(x)

    # RNNs
    x = layers.Bidirectional(layers.LSTM(
        128, return_sequences=True, dropout=0.4))(x)
    x = layers.Bidirectional(layers.LSTM(
        64, return_sequences=True, dropout=0.4))(x)

    # Output layer
    x = layers.Dense(
        len(char_to_num.get_vocabulary()) + 1, activation="softmax", name="dense2"
    )(x)

    # Add CTC layer for calculating CTC loss at each step
    output = CTCLayer(name="ctc_loss")(labels, x)

    # Define the model
    model = keras.models.Model(
        inputs=[input_img, labels], outputs=output, name="ocr_model_v1"
    )
    # Optimizer
    opt = keras.optimizers.Adam()
    # Compile the model and return
    model.compile(optimizer=opt)
    return model


# A utility function to decode the output of the network


def decode_batch_predictions(pred):
    input_len = np.ones(pred.shape[0]) * pred.shape[1]
    # Use greedy search. For complex tasks, you can use beam search
    results = keras.backend.ctc_decode(pred, input_length=input_len, greedy=True)[0][0][
        :, :max_length
    ]
    # Iterate over the results and get back the text
    output_text = []
    for res in results:
        res = tf.strings.reduce_join(num_to_char(res)).numpy().decode("utf-8")
        output_text.append(res)
    return output_text


#  Let's check results on some validation samples
def validate_results(training=False, validation_dataset=None):
    for batch in validation_dataset.take(1):
        batch_images = batch["image"]
        batch_labels = batch["label"]

        preds = prediction_model.predict(batch_images)
        pred_texts = decode_batch_predictions(preds)

        if training:

            orig_texts = []
            for label in batch_labels:
                label = tf.strings.reduce_join(
                    num_to_char(label)).numpy().decode("utf-8")
                orig_texts.append(label)

            _, ax = plt.subplots(4, 4, figsize=(15, 5))
            for i in range(len(pred_texts)):
                img = (batch_images[i, :, :, 0] * 255).numpy().astype(np.uint8)
                img = img.T
                title = f"Prediction: {pred_texts[i]}"
                ax[i // 4, i % 4].imshow(img, cmap="gray")
                ax[i // 4, i % 4].set_title(title)
                ax[i // 4, i % 4].axis("off")

            plt.show()

        else:

            return pred_texts


if __name__ == '__main__':

    if training:
        # Get the model
        model = build_model()
        model.summary()

        # Training
        epochs = 1000
        early_stopping_patience = 40
        # Add early stopping
        early_stopping = keras.callbacks.EarlyStopping(
            monitor="val_loss", patience=early_stopping_patience, restore_best_weights=True
        )

        # Train the model
        history = model.fit(
            train_dataset,
            validation_data=validation_dataset,
            epochs=epochs,
            callbacks=[early_stopping, tensorboard_callback, cp_callback],
        )

        model.save(model_save_dir)

        # Get the prediction model by extracting layers till the output layer
        prediction_model = keras.models.Model(
            model.get_layer(name="image").input, model.get_layer(
                name="dense2").output
        )
        prediction_model.summary()

        validate_results(training=True, validation_dataset=validation_dataset)

    else:
        model = tf.keras.models.load_model(model_save_dir)

        prediction_model = keras.models.Model(
            model.get_layer(name="image").input, model.get_layer(
                name="dense2").output
        )

        x_images, labels, characters = get_lst_images(data_dir_prod)

        validation_dataset = tf.data.Dataset.from_tensor_slices(
            (np.array(x_images), np.array(labels)))
        validation_dataset = (
            validation_dataset.map(
                encode_single_sample, num_parallel_calls=tf.data.AUTOTUNE
            )
            .batch(1)
            .prefetch(buffer_size=tf.data.AUTOTUNE)
        )
        # prediction_model.summary()
        pred_texts = validate_results(
            training=training, validation_dataset=validation_dataset)

        print('t')
