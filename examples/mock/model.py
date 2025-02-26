import torch
import torch.nn as nn
import onnx
import onnxruntime as ort
import numpy as np

class MultiplyByTwoModel(nn.Module):
    def __init__(self):
        super(MultiplyByTwoModel, self).__init__()

    def forward(self, x):
        return x * 2  # Просто умножаем входной вектор на 2

if __name__ == "__main__":
    # 1. Создаем и тестируем PyTorch модель
    model = MultiplyByTwoModel()
    input_tensor = torch.tensor([1.0, 2.0, 3.0])

    # Проверка работы модели
    output_tensor = model(input_tensor)
    print("PyTorch Output:", output_tensor)

    # 2. Экспорт модели в ONNX
    onnx_path = "model.onnx"
    dummy_input = torch.randn(3)  # Должен совпадать с форматом входных данных
    torch.onnx.export(model, dummy_input, onnx_path,
                      input_names=["input"], output_names=["output"],
                      dynamic_axes={"input": {0: "batch_size"}, "output": {0: "batch_size"}})

    print(f"Model exported to {onnx_path}")

    # 3. Инференс с ONNX Runtime
    session = ort.InferenceSession(onnx_path)
    onnx_input = np.array([1.0, 2.0, 3.0], dtype=np.float32).reshape(1, -1)  # ONNX требует правильной формы
    onnx_output = session.run(None, {"input": onnx_input})[0]

    print("ONNX Output:", onnx_output)
